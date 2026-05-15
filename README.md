# Boson WebGateway Client (HiggsNode)

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://adoptium.net/)
[![Maven](https://img.shields.io/badge/Maven-3.8%2B-red.svg)](https://maven.apache.org/)

The Java client library for the **Boson WebGateway** service — a light DHT node implementation (codename *HiggsNode*) that gives browser-based and other HTTP-only clients full access to the Boson DHT network through a super node's HTTP API.

---

## Table of Contents

- [Boson Node Types](#boson-node-types)
- [What Is WebGateway?](#what-is-webgateway)
- [How It Works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Build](#build)
- [Adding as a Dependency](#adding-as-a-dependency)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

---

## Boson Node Types

The Boson network defines three node roles:

| Node Type | DHT Participation | Service Access | Typical Use Case                    |
|---|---|---|-------------------------------------|
| **Super Node** | Full DHT node | Hosts layer-2 services | Server infrastructure               |
| **Regular Node** | Full DHT node (UDP) | Consumes services | e.g.; Native desktop / mobile apps        |
| **Light Node** | None — uses WebGateway | Consumes services | e.g.; Web apps running in a browser |

A **Light Node** cannot run a full DHT implementation (for example, a browser cannot open UDP sockets), so it delegates all DHT operations to the WebGateway HTTP API exposed by a super node. HiggsNode is the Light Node implementation for Java.

---

## What Is WebGateway?

WebGateway is a Boson layer-2 service that runs on a super node alongside the DHT. It wraps the DHT's UDP-based operations as authenticated HTTPS endpoints, enabling clients that cannot speak UDP to:

- Look up and announce peer service registrations (`FIND_PEER` / `ANNOUNCE_PEER`)
- Look up and store values in the DHT (`FIND_VALUE` / `STORE_VALUE`)
- Look up DHT nodes (`FIND_NODE`)

The service uses a custom **Compact Web Token (CWT)** for authentication — a CBOR-encoded, Ed25519-signed bearer token derived from the client's user or device key. Rate limiting and per-client access control are enforced by the gateway.

This repository contains the **client side** of WebGateway: the `HiggsNode` class, which implements the same `Node` interface as the full `KadNode`, making it a transparent drop-in replacement. Any Boson service or application written against the `Node` interface works without modification on top of HiggsNode.

---

## How It Works

```
  Web App / Light Client
       │
       │  HTTPS (TLS 1.3)
       │  Bearer: Compact Web Token (CBOR + Ed25519 signature)
       ▼
 ┌─────────────────────────────────────┐
 │  WebGateway (super node service)    │  ← public HTTPS endpoint
 │  Rate limit · Auth · CORS           │
 └─────────────────────────────────────┘
       │  Internal calls
       ▼
 ┌─────────────────────────────────────┐
 │  KadNode (full DHT node)            │  ← UDP port 39001
 │  Boson Kademlia DHT                 │
 └─────────────────────────────────────┘
```

1. **Identity** — the client holds an Ed25519 key pair. Two modes are supported:
   - *User key mode*: the user's private key is held directly; the user ID is the public key. Suitable when the full key is available.
   - *User ID + device key mode*: only the user's public key (ID) and a device-specific private key are held. The access token identifies the user but is signed by the device key.
2. **Access token** — before each HTTP request, HiggsNode generates a short-lived CBOR token containing the issuer, audience (gateway node ID), subject (user ID), optional device ID, expiry, and a random nonce. The token is signed with the identity key and sent as an HTTP `Authorization: Bearer` header.
3. **TLS trust** — when connecting over HTTPS, HiggsNode uses a `HybridTrustManager` that validates the server's self-signed certificate against the expected gateway peer ID. No CA installation is required.
4. **DHT operations** — HiggsNode translates each `Node` interface call into the corresponding REST request and returns the same result types as `KadNode`.
5. **Local data** — values and peer records stored via HiggsNode are cached locally. Persistent entries are periodically reannounced to prevent DHT expiry.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Java JDK | 17 or later |
| Apache Maven | 3.8 or later |
| Boson Core (`boson-api`) | same version or compatible |
| A running Boson super node with WebGateway | — |

---

## Build

```bash
git clone https://github.com/bosonnetwork/Boson.WebGateway.Client.git
cd Boson.WebGateway.Client
./mvnw clean package
```

The compiled JAR is placed in `target/lib/higgs-java-<version>.jar`.

To skip tests:

```bash
./mvnw clean package -DskipTests
```

---

## Adding as a Dependency

Add the following to your Maven `pom.xml`:

```xml
<dependency>
    <groupId>io.bosonnetwork</groupId>
    <artifactId>higgs-java</artifactId>
    <version>${boson.version}</version>
</dependency>
```

---

## Usage

### User key mode

Use this when the application holds the full user private key (e.g., a trusted native client or server-side process).

```java
// Gateway coordinates — obtain from the super node operator.
Id gatewayNodeId = Id.of("HZXXs9LTfNQjrDKvvexRhuMk8TTJhYCfrHwaj3jUzuhZ");
Id gatewayPeerId = Id.of("GbRwG3WgKgApSDBr9FGo5Y3RssSWxfWhanXMBdPCo5F2");
String gatewayUrl = "https://gateway.example.com:8443";

// Build the light node.
HiggsNode node = HiggsNode.builder()
    .userKey("<Base58-or-0x-hex-Ed25519-private-key>")
    .gatewayNodeId(gatewayNodeId)
    .gatewayPeerId(gatewayPeerId)
    .gatewayUrl(gatewayUrl)
    .build();

// Start — establishes the HTTPS connection and verifies the gateway version.
node.start().get();

// Use the Node interface exactly as you would with KadNode.
List<PeerInfo> peers = node.findPeer(serviceId, -1, 1, LookupOption.ARBITRARY).get();

node.stop().get();
```

### User ID + device key mode

Use this when the full user private key should not be stored on the device. The device holds only its own private key; the user ID (public key) is stored separately.

```java
Id userId    = Id.of("<Base58-user-public-key>");

HiggsNode node = HiggsNode.builder()
    .userId(userId)
    .deviceKey("<Base58-device-private-key>")
    .gatewayNodeId(gatewayNodeId)
    .gatewayPeerId(gatewayPeerId)
    .gatewayUrl(gatewayUrl)
    .build();

node.start().toCompletionStage().toCompletableFuture().get();
```

### Providing an external Vert.x instance

```java
Vertx vertx = Vertx.vertx();

HiggsNode node = HiggsNode.builder()
    .vertx(vertx)
    .userKey("<Base58-private-key>")
    .gatewayNodeId(gatewayNodeId)
    .gatewayPeerId(gatewayPeerId)
    .gatewayUrl(gatewayUrl)
    .build();
```

If no `Vertx` instance is provided, HiggsNode creates an internal one on `start()`.

### Using HiggsNode with Boson services

Because `HiggsNode` implements `Node`, it can be passed directly to any Boson service or client library that accepts a `Node`:

```java
// Pass the light node to the messaging client — no code change needed.
MessagingClient client = MessagingClient.create(node, messagingConfig);
```

---

## Contributing

We welcome contributions from the open-source community. To get started:

1. Fork this repository and create a feature branch.
2. Make your changes and add tests where applicable.
3. Ensure `./mvnw clean verify` passes.
4. Open a pull request with a clear description of the change.

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before contributing.

---

## License

This project is licensed under the [MIT License](LICENSE).