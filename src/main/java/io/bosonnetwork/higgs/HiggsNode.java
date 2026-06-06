/*
 * Copyright (c) 2023 -      bosonnetwork.io
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.bosonnetwork.higgs;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.TrustOptions;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.bosonnetwork.ConnectionStatusListener;
import io.bosonnetwork.CryptoContext;
import io.bosonnetwork.Id;
import io.bosonnetwork.Identity;
import io.bosonnetwork.LookupOption;
import io.bosonnetwork.Network;
import io.bosonnetwork.Node;
import io.bosonnetwork.NodeInfo;
import io.bosonnetwork.PeerInfo;
import io.bosonnetwork.Result;
import io.bosonnetwork.Value;
import io.bosonnetwork.crypto.CryptoException;
import io.bosonnetwork.crypto.CryptoIdentity;
import io.bosonnetwork.crypto.HybridTrustManager;
import io.bosonnetwork.crypto.Signature;
import io.bosonnetwork.cwt.SignedCwt;
import io.bosonnetwork.json.Json;
import io.bosonnetwork.service.AccessScope;
import io.bosonnetwork.utils.Base58;
import io.bosonnetwork.utils.Hex;
import io.bosonnetwork.vertx.ContextualFuture;

/**
 * A lightweight {@link Node} implementation backed by a Boson Web Gateway's authenticated
 * HTTP/REST API.
 * <p>
 * Instead of joining the DHT directly, a {@code HiggsNode} forwards {@code Node} operations
 * (find/store value, find/announce peer) to a gateway service, and additionally exposes the
 * gateway's per-user persistent storage ({@link #getValue}, {@link #removeValue},
 * {@link #getPeers}, {@link #getPeer}, {@link #removePeers}, {@link #removePeer}). It keeps no
 * routing table or local storage, so it is cheap to create and suited to clients (mobile, CLI,
 * embedded) that cannot run a full node.
 *
 * <h2>Authentication</h2>
 * Every request carries a short-lived {@link io.bosonnetwork.cwt.SignedCwt CWT} bearer token.
 * A node uses one of two mutually exclusive modes, selected at build time:
 * <ul>
 *   <li><b>User-key mode</b> ({@link Builder#userKey}): the client signs tokens directly as the user.</li>
 *   <li><b>Device mode</b> ({@link Builder#userId} + {@link Builder#deviceKey}): the client signs as a
 *       device acting on behalf of the user, setting the token's client id to the device id.</li>
 * </ul>
 *
 * <h2>Gateway binding</h2>
 * The target gateway is identified by its node id, peer id and URL ({@link Builder#gatewayNodeId},
 * {@link Builder#gatewayPeerId}, {@link Builder#gatewayUrl}). {@link #start()} fetches the gateway's
 * {@code /info} and verifies the returned node and peer ids match the configured values, failing fast
 * on mismatch; over HTTPS the gateway's self-signed certificate is pinned to its peer id.
 *
 * <h2>Lifecycle &amp; threading</h2>
 * Call {@link #start()} before issuing requests and {@link #stop()} when finished; requests made while
 * not running fail with {@link IllegalStateException}. Built on Vert.x — the returned
 * {@link ContextualFuture}s complete on the caller's Vert.x context.
 *
 * <h2>Unsupported operations</h2>
 * Some {@link Node} methods are not meaningful for a gateway client: {@link #getNodeInfo()} throws
 * {@link UnsupportedOperationException}, while {@link #bootstrap(NodeInfo) bootstrap},
 * {@link #addConnectionStatusListener} and {@link #removeConnectionStatusListener} are no-ops.
 *
 * <p>Instances are obtained through {@link #builder()}.
 */
public class HiggsNode implements Node {
	private static final long ACCESS_TOKEN_TIMEOUT = 10 * 60 * 1000;

	private static final String VERSION = "Higgs/1";

	private final Vertx vertx;

	// The active signing identity used for access tokens and crypto operations:
	// the user identity in user-key mode, or the device identity in device mode.
	private final Identity identity;

	// user identity
	private final Identity userIdentity;
	// or user id and device identity
	private final Id userId;
	private final Identity deviceIdentity;

	private final Id gatewayNodeId;
	private final Id gatewayPeerId;
	private final URL gatewayUrl;

	private LookupOption defaultLookupOption;

	private String gatewayVersion;
	private WebClient webClient;

	private volatile AccessTokenCache tokenCache;

	private final AtomicBoolean running;

	private static final Logger log = LoggerFactory.getLogger(HiggsNode.class);

	private record AccessTokenCache(String token, long createdAt) {}

	private HiggsNode(Vertx vertx, Signature.KeyPair userKey, Id gatewayNodeId, Id gatewayPeerId, URL gatewayUrl) {
		this.vertx = vertx;
		this.userIdentity = new CryptoIdentity(userKey);
		this.userId = userIdentity.getId();
		this.deviceIdentity = null;
		this.gatewayNodeId = gatewayNodeId;
		this.gatewayPeerId = gatewayPeerId;
		this.gatewayUrl = gatewayUrl;
		this.defaultLookupOption = LookupOption.ARBITRARY;

		this.identity = userIdentity;

		this.running = new AtomicBoolean(false);
	}

	private HiggsNode(Vertx vertx, Id userId, Signature.KeyPair deviceKey, Id gatewayNodeId, Id gatewayPeerId, URL gatewayUrl) {
		this.vertx = vertx;
		this.userIdentity = null;
		this.userId = userId;
		this.deviceIdentity = new CryptoIdentity(deviceKey);
		this.gatewayNodeId = gatewayNodeId;
		this.gatewayPeerId = gatewayPeerId;
		this.gatewayUrl = gatewayUrl;
		this.defaultLookupOption = LookupOption.ARBITRARY;

		this.identity = deviceIdentity;

		this.running = new AtomicBoolean(false);
	}

	@Override
	public Id getId() {
		return identity.getId();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @implNote Unsupported by the gateway client: a {@code HiggsNode} is a thin REST client with
	 *           no local DHT node, so it has no {@link NodeInfo} of its own. Always throws
	 *           {@link UnsupportedOperationException}.
	 */
	@Override
	public Result<NodeInfo> getNodeInfo() {
		throw new UnsupportedOperationException("getNodeInfo");
	}

	@Override
	public String getVersion() {
		return VERSION + ":" + (gatewayVersion != null ? gatewayVersion : "N/A");
	}

	@Override
	public void setDefaultLookupOption(LookupOption option) {
		Objects.requireNonNull(option, "option");
		this.defaultLookupOption = option;
	}

	@Override
	public LookupOption getDefaultLookupOption() {
		return defaultLookupOption;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @implNote No-op in the gateway client: it holds no persistent DHT connection whose status
	 *           could change, so the registered listener is never invoked.
	 */
	@Override
	public void addConnectionStatusListener(ConnectionStatusListener listener) {
	}

	/**
	 * {@inheritDoc}
	 *
	 * @implNote No-op in the gateway client (see {@link #addConnectionStatusListener}).
	 */
	@Override
	public void removeConnectionStatusListener(ConnectionStatusListener listener) {
	}

	@Override
	public ContextualFuture<Void> start() {
		if (!running.compareAndSet(false, true))
			return ContextualFuture.failedFuture(new IllegalStateException("Already started"));

		boolean ssl = gatewayUrl.getProtocol().equals("https");
		WebClientOptions options = new WebClientOptions()
				.setSsl(ssl)
				.setDefaultHost(gatewayUrl.getHost())
				.setDefaultPort(gatewayUrl.getPort() > 0 ? gatewayUrl.getPort() : gatewayUrl.getDefaultPort())
				.setProtocolVersion(HttpVersion.HTTP_1_1);

		if (ssl) {
			options.setEnabledSecureTransportProtocols(Set.of("TLSv1.3"))
					.setTrustOptions(TrustOptions.wrap(new HybridTrustManager(gatewayPeerId.toString(), gatewayPeerId.bytesUnsafe())));
		}

		webClient = WebClient.create(vertx, options);

		Future<Void> future = getGatewayInfo().compose(info -> {
			Id nodeId = Id.of(info.getString("nodeId"));
			if (!nodeId.equals(gatewayNodeId))
				return Future.failedFuture(new HiggsException(HiggsException.NO_HTTP_STATUS, "Gateway node ID mismatch"));
			Id peerId = Id.of(info.getString("peerId"));
			if (!peerId.equals(gatewayPeerId))
				return Future.failedFuture(new HiggsException(HiggsException.NO_HTTP_STATUS, "Gateway peer ID mismatch"));

			gatewayVersion = info.getString("version");
			return Future.succeededFuture();
		}).onFailure(e -> {
			log.error("Failed to get gateway info", e);
			webClient.close();
			webClient = null;
			running.set(false);
		}).mapEmpty();

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Void> stop() {
		if (!running.compareAndSet(true, false))
			return ContextualFuture.failedFuture(new IllegalStateException("Not started"));

		if (webClient != null) {
			webClient.close();
			webClient = null;
		}

		return ContextualFuture.succeededFuture();
	}

	@Override
	public boolean isRunning() {
		return running.get();
	}

	private void runningCheck() {
		if (!isRunning())
			throw new IllegalStateException("Node not running");
	}

	/**
	 * {@inheritDoc}
	 *
	 * @implNote No-op in the gateway client: routing-table bootstrapping is the gateway's
	 *           responsibility, not the client's. Returns an already-completed future without
	 *           contacting any node.
	 */
	@Override
	public ContextualFuture<Void> bootstrap(NodeInfo node) {
		return ContextualFuture.succeededFuture();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @implNote No-op in the gateway client (see {@link #bootstrap(NodeInfo)}).
	 */
	@Override
	public ContextualFuture<Void> bootstrap(Collection<NodeInfo> bootstrapNodes) {
		return ContextualFuture.succeededFuture();
	}

	@Override
	public ContextualFuture<Result<NodeInfo>> findNode(Id id, LookupOption option) {
		Objects.requireNonNull(id, "id");
		runningCheck();

		final LookupOption lookupOption = option != null ? option : defaultLookupOption;

		Future<Result<NodeInfo>> future = webClient.get("/nodes/" + id)
				.addQueryParam("mode", lookupOption.name().toLowerCase())
				.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200) {
						JsonObject body = res.bodyAsJsonObject();
						NodeInfo n4 = body.containsKey(Network.IPv4.name()) ?
								body.getJsonObject(Network.IPv4.name()).mapTo(NodeInfo.class) : null;
						NodeInfo n6 = body.containsKey(Network.IPv6.name()) ?
								body.getJsonObject(Network.IPv6.name()).mapTo(NodeInfo.class) : null;
						if (n4 == null && n6 == null)
							return Future.succeededFuture();
						else
							return Future.succeededFuture(Result.of(n4, n6));
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture();
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover(e -> {
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(e instanceof HiggsException he ? he :
							new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Value> findValue(Id id, int expectedSequenceNumber, LookupOption option) {
		Objects.requireNonNull(id, "id");
		if (expectedSequenceNumber < -1)
			throw new IllegalArgumentException("expectedSequenceNumber must be >= -1");
		runningCheck();

		final LookupOption lookupOption = option != null ? option : defaultLookupOption;

		HttpRequest<Buffer> request = webClient.get("/values/" + id);
		request.addQueryParam("mode", lookupOption.name().toLowerCase());
		if (expectedSequenceNumber >= 0)
			request.addQueryParam("seq", String.valueOf(expectedSequenceNumber));

		Future<Value> future = request.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200) {
						JsonObject body = res.bodyAsJsonObject();
						return Future.succeededFuture(body.mapTo(Value.class));
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture();
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover((e) -> {
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(e instanceof HiggsException he ? he :
							new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Void> storeValue(Value value, int expectedSequenceNumber, boolean persistent) {
		Objects.requireNonNull(value, "value");
		if (expectedSequenceNumber < -1)
			throw new IllegalArgumentException("expectedSequenceNumber must be >= -1");
		if (!value.isValid())
			throw new IllegalArgumentException("Value is invalid");
		runningCheck();

		JsonObject body = new JsonObject();
		if (expectedSequenceNumber >= 0)
			body.put("expectedSequenceNumber", expectedSequenceNumber);
		if (persistent)
			body.put("persistent", true);
		body.put("value", value);

		Future<Void> future = webClient.post("/values")
				.bearerTokenAuthentication(getAccessToken())
				.sendJsonObject(body)
				.compose(res -> {
					if (res.statusCode() == 201) {
						return Future.<Void>succeededFuture();
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover(e -> {
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(e instanceof HiggsException he ? he :
							new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<List<PeerInfo>> findPeer(Id id, int expectedSequenceNumber, int expectedCount, LookupOption option) {
		Objects.requireNonNull(id, "id");
		if (expectedSequenceNumber < -1)
			throw new IllegalArgumentException("expectedSequenceNumber must be >= -1");
		if (expectedCount < 0)
			throw new IllegalArgumentException("expectedCount must be >= 0");
		runningCheck();

		final LookupOption lookupOption = option != null ? option : defaultLookupOption;

		HttpRequest<Buffer> request = webClient.get("/peers/" + id);
		request.addQueryParam("mode", lookupOption.name().toLowerCase());
		if (expectedSequenceNumber >= 0)
			request.addQueryParam("seq", Integer.toString(expectedSequenceNumber));
		request.addQueryParam("count", Integer.toString(expectedCount));

		Future<List<PeerInfo>> future = request.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200) {
						JsonArray body = res.bodyAsJsonArray();
						List<PeerInfo> result = new ArrayList<>(body.size());
						for (Object o : body) {
							// Fail the future on a malformed element rather than throwing a checked
							// HiggsException from a stream lambda.
							if (!(o instanceof JsonObject jo))
								return Future.failedFuture(new HiggsException(HiggsException.NO_HTTP_STATUS, "Malformed peer in gateway response"));
							result.add(Json.objectMapper().convertValue(jo.getMap(), PeerInfo.class));
						}
						return Future.succeededFuture(result);
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture(List.of());
					} else {
						return Future.<List<PeerInfo>>failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover(e -> {
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(e instanceof HiggsException he ? he :
							new HiggsException("Gateway request failed", e));
				});


		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Void> announcePeer(PeerInfo peer, int expectedSequenceNumber, boolean persistent) {
		Objects.requireNonNull(peer, "peer");
		if (expectedSequenceNumber < -1)
			throw new IllegalArgumentException("expectedSequenceNumber must be >= -1");
		if (!peer.isValid())
			throw new IllegalArgumentException("Peer is invalid");
		runningCheck();

		JsonObject body = new JsonObject();
		if (expectedSequenceNumber >= 0)
			body.put("expectedSequenceNumber", expectedSequenceNumber);
		if (persistent)
			body.put("persistent", true);
		body.put("peer", peer);

		Future<Void> future = webClient.post("/peers")
				.bearerTokenAuthentication(getAccessToken())
				.sendJsonObject(body)
				.compose(res -> {
					if (res.statusCode() == 201)
						return Future.<Void>succeededFuture();
					else
						return Future.failedFuture(wrapErrorResponseToException(res));
				})
				.recover((e) -> {
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(e instanceof HiggsException he ? he :
							new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Value> getValue(Id valueId) {
		Objects.requireNonNull(valueId, "valueId");
		runningCheck();

		Future<Value> future = webClient.get("/user/values/" + valueId)
				.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200) {
						JsonObject body = res.bodyAsJsonObject();
						return Future.succeededFuture(body.mapTo(Value.class));
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture();
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover((e) -> {
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(e instanceof HiggsException he ? he :
							new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Boolean> removeValue(Id valueId) {
		Objects.requireNonNull(valueId, "valueId");
		runningCheck();

		Future<Boolean> future = webClient.delete("/user/values/" + valueId)
				.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 204) {
						return Future.succeededFuture(true);
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture(false);
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover((e) -> {
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(e instanceof HiggsException he ? he :
							new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<List<PeerInfo>> getPeers(Id peerId) {
		Objects.requireNonNull(peerId, "peerId");
		runningCheck();

		Future<List<PeerInfo>> future = webClient.get("/user/peers/" + peerId)
				.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200) {
						JsonArray body = res.bodyAsJsonArray();
						List<PeerInfo> result = new ArrayList<>(body.size());
						for (Object o : body) {
							// Fail the future on a malformed element rather than throwing a checked
							// HiggsException from a stream lambda.
							if (!(o instanceof JsonObject jo))
								return Future.failedFuture(new HiggsException(HiggsException.NO_HTTP_STATUS, "Malformed peer in gateway response"));
							result.add(Json.objectMapper().convertValue(jo.getMap(), PeerInfo.class));
						}
						return Future.succeededFuture(result);
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture(List.of());
					} else {
						return Future.<List<PeerInfo>>failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover(e -> {
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(e instanceof HiggsException he ? he :
							new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Boolean> removePeers(Id peerId) {
		Objects.requireNonNull(peerId, "peerId");
		runningCheck();

		Future<Boolean> future = webClient.delete("/user/peers/" + peerId)
				.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 204) {
						return Future.succeededFuture(true);
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture(false);
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover((e) -> {
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(e instanceof HiggsException he ? he :
							new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<PeerInfo> getPeer(Id peerId, long fingerprint) {
		Objects.requireNonNull(peerId, "peerId");
		runningCheck();

		Future<PeerInfo> future = webClient.get("/user/peers/" + peerId + "/" + fingerprint)
				.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200) {
						return Future.succeededFuture(res.bodyAsJson(PeerInfo.class));
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture();
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover(e -> {
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(e instanceof HiggsException he ? he :
							new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Boolean> removePeer(Id peerId, long fingerprint) {
		Objects.requireNonNull(peerId, "peerId");
		runningCheck();

		Future<Boolean> future = webClient.delete("/user/peers/" + peerId + "/" + fingerprint)
				.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 204) {
						return Future.succeededFuture(true);
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture(false);
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover((e) -> {
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(e instanceof HiggsException he ? he :
							new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public byte[] sign(byte[] data) {
		Objects.requireNonNull(data, "data");
		return identity.sign(data);
	}

	@Override
	public boolean verify(byte[] data, byte[] signature) {
		Objects.requireNonNull(data, "data");
		Objects.requireNonNull(signature, "signature");
		return identity.verify(data, signature);
	}

	@Override
	public byte[] encrypt(Id recipient, byte[] data) throws CryptoException {
		Objects.requireNonNull(recipient, "recipient");
		Objects.requireNonNull(data, "data");
		return identity.encrypt(recipient, data);
	}

	@Override
	public byte[] encrypt(Id recipient, byte[] nonce, byte[] data) throws CryptoException {
		Objects.requireNonNull(recipient, "recipient");
		Objects.requireNonNull(nonce, "nonce");
		Objects.requireNonNull(data, "data");
		return identity.encrypt(recipient, nonce, data);
	}

	@Override
	public byte[] decrypt(Id sender, byte[] data) throws CryptoException {
		Objects.requireNonNull(sender, "sender");
		Objects.requireNonNull(data, "data");
		return identity.decrypt(sender, data);
	}

	@Override
	public byte[] decrypt(Id sender, byte[] nonce, byte[] data) throws CryptoException {
		Objects.requireNonNull(sender, "sender");
		Objects.requireNonNull(nonce, "nonce");
		Objects.requireNonNull(data, "data");
		return identity.decrypt(sender, nonce, data);
	}

	@Override
	public CryptoContext createCryptoContext(Id id) throws CryptoException {
		Objects.requireNonNull(id, "id");
		return identity.createCryptoContext(id);
	}

	@Override
	public <T> T unwrap(Class<T> clazz) {
		if (clazz.isInstance(vertx))
			return clazz.cast(vertx);

		return null;
	}

	private Future<JsonObject> getGatewayInfo() {
		return webClient.get("/info")
				.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200)
						return Future.succeededFuture(res.bodyAsJsonObject());
					else
						return Future.failedFuture(wrapErrorResponseToException(res));
				});
	}

	private String getAccessToken() {
		AccessTokenCache tc = tokenCache;
		if (tc == null || System.currentTimeMillis() - tc.createdAt > ACCESS_TOKEN_TIMEOUT) {
			SignedCwt.Builder builder = SignedCwt.builder(identity)
					.subject(userId)
					.audience(gatewayPeerId)
					.expiration(Duration.ofMillis(ACCESS_TOKEN_TIMEOUT + 1000 * 60))
					.notBeforeNow()
					.issuedAtNow()
					.scope(AccessScope.CLIENT.toString());
			if (deviceIdentity != null)
				builder.clientId(deviceIdentity.getId());

			String token = builder.buildToString();
			tc = new AccessTokenCache(token, System.currentTimeMillis());
			tokenCache = tc;
		}

		return tc.token;
	}

	private HiggsException wrapErrorResponseToException(HttpResponse<Buffer> res) {
		String body = res.bodyAsString();
		if (res.statusCode() == 401) {
			// noinspection LoggingSimilarMessage
			log.debug("HTTP status: {}, Unauthorized. {}", res.statusCode(), body);
		} else if (res.statusCode() == 429) {
			// noinspection LoggingSimilarMessage
			log.debug("HTTP status: {}, Too Many Requests. {}", res.statusCode(), body);
		} else if (res.statusCode() == 504) {
			log.debug("HTTP status: {}, Gateway timeout. {}", res.statusCode(), body);
		} else {
			// noinspection LoggingSimilarMessage
			log.error("HTTP status: {}, {}", res.statusCode(), body);
		}

		return new HiggsException(res.statusCode(), body);
	}

	/**
	 * Creates a new {@link Builder} for constructing a {@code HiggsNode}.
	 *
	 * @return a new builder
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Fluent builder for {@link HiggsNode}.
	 * <p>
	 * Exactly one authentication mode must be configured: either {@link #userKey(Signature.KeyPair)
	 * userKey} (user-key mode), or {@link #userId(Id) userId} together with
	 * {@link #deviceKey(Signature.KeyPair) deviceKey} (device mode). The gateway coordinates
	 * {@link #gatewayNodeId(Id) gatewayNodeId}, {@link #gatewayPeerId(Id) gatewayPeerId} and
	 * {@link #gatewayUrl(URL) gatewayUrl} are all required. {@link #build()} validates the
	 * configuration and throws {@link IllegalStateException} if anything required is missing.
	 * A {@link Vertx} instance may be supplied via {@link #vertx(Vertx)}. Not thread-safe.
	 */
	public static class Builder {
		private Vertx vertx;
		// user key
		private Signature.KeyPair userKey;
		// or user id and device key
		private Id userId;
		private Signature.KeyPair deviceKey;
		// gateway id and url
		private Id gatewayNodeId;
		private Id gatewayPeerId;
		private URL gatewayUrl;

		private Builder() {
		}

		/**
		 * Sets the Vert.x instance the node will run on.
		 *
		 * @param vertx the Vert.x instance (must not be {@code null})
		 * @return this builder
		 */
		public Builder vertx(Vertx vertx) {
			Objects.requireNonNull(vertx, "vertx");
			this.vertx = vertx;
			return this;
		}

		/**
		 * Selects user-key mode using the given user key pair (the client signs tokens as the user).
		 * Mutually exclusive with device mode ({@link #userId}/{@link #deviceKey}).
		 *
		 * @param key the user key pair (must not be {@code null})
		 * @return this builder
		 */
		public Builder userKey(Signature.KeyPair key) {
			Objects.requireNonNull(key, "key");
			this.userKey = key;
			return this;
		}

		/**
		 * Selects user-key mode from an encoded private key string.
		 *
		 * @param privateKey the user private key, either a {@code 0x}-prefixed hex string or a Base58 string
		 * @return this builder
		 */
		public Builder userKey(String privateKey) {
			Objects.requireNonNull(privateKey, "privateKey");
			byte[] sk = privateKey.startsWith("0x") ? Hex.decode(privateKey.substring(2)) : Base58.decode(privateKey);
			return userKey(Signature.KeyPair.fromPrivateKey(sk));
		}

		/**
		 * Selects user-key mode from a raw private key.
		 *
		 * @param privateKey the user private key bytes (must be {@link Signature.PrivateKey#BYTES} long)
		 * @return this builder
		 * @throws IllegalArgumentException if the key length is invalid
		 */
		public Builder userKey(byte[] privateKey) {
			Objects.requireNonNull(privateKey, "privateKey");
			if (privateKey.length != Signature.PrivateKey.BYTES)
				throw new IllegalArgumentException("Invalid private key");

			return userKey(Signature.KeyPair.fromPrivateKey(privateKey));
		}

		/**
		 * Sets the user id for device mode. Used together with {@link #deviceKey}: the device signs
		 * tokens on behalf of this user.
		 *
		 * @param userId the user id (must not be {@code null})
		 * @return this builder
		 */
		public Builder userId(Id userId) {
			Objects.requireNonNull(userId, "userId");
			this.userId = userId;
			return this;
		}

		/**
		 * Selects device mode using the given device key pair. Used together with {@link #userId}.
		 * Mutually exclusive with user-key mode ({@link #userKey}).
		 *
		 * @param key the device key pair (must not be {@code null})
		 * @return this builder
		 */
		public Builder deviceKey(Signature.KeyPair key) {
			Objects.requireNonNull(key, "key");
			this.deviceKey = key;
			return this;
		}

		/**
		 * Selects device mode from an encoded private key string.
		 *
		 * @param privateKey the device private key, either a {@code 0x}-prefixed hex string or a Base58 string
		 * @return this builder
		 */
		public Builder deviceKey(String privateKey) {
			Objects.requireNonNull(privateKey, "privateKey");
			byte[] sk = privateKey.startsWith("0x") ? Hex.decode(privateKey.substring(2)) : Base58.decode(privateKey);
			return deviceKey(Signature.KeyPair.fromPrivateKey(sk));
		}

		/**
		 * Selects device mode from a raw private key.
		 *
		 * @param privateKey the device private key bytes (must be {@link Signature.PrivateKey#BYTES} long)
		 * @return this builder
		 * @throws IllegalArgumentException if the key length is invalid
		 */
		public Builder deviceKey(byte[] privateKey) {
			Objects.requireNonNull(privateKey, "privateKey");
			if (privateKey.length != Signature.PrivateKey.BYTES)
				throw new IllegalArgumentException("Invalid private key");

			return deviceKey(Signature.KeyPair.fromPrivateKey(privateKey));
		}

		/**
		 * Sets the expected node id of the target gateway. Verified against the gateway's
		 * {@code /info} response on {@link HiggsNode#start()}.
		 *
		 * @param id the gateway node id (must not be {@code null})
		 * @return this builder
		 */
		public Builder gatewayNodeId(Id id) {
			Objects.requireNonNull(id, "gatewayNodeId");
			this.gatewayNodeId = id;
			return this;
		}

		/**
		 * Sets the expected peer id of the target gateway. Verified on {@link HiggsNode#start()} and,
		 * over HTTPS, pinned in the TLS trust check.
		 *
		 * @param id the gateway peer id (must not be {@code null})
		 * @return this builder
		 */
		public Builder gatewayPeerId(Id id) {
			Objects.requireNonNull(id, "gatewayPeerId");
			this.gatewayPeerId = id;
			return this;
		}

		/**
		 * Sets the base URL of the target gateway.
		 *
		 * @param url an {@code http} or {@code https} URL (must not be {@code null})
		 * @return this builder
		 * @throws IllegalArgumentException if the URL uses a non-http(s) protocol
		 */
		public Builder gatewayUrl(URL url) {
			Objects.requireNonNull(url, "url");
			if (!url.getProtocol().equals("http") && !url.getProtocol().equals("https"))
				throw new IllegalArgumentException("Invalid gateway URL protocol (must be http or https): " + url.getProtocol());
			this.gatewayUrl = url;
			return this;
		}

		/**
		 * Sets the base URL of the target gateway from a string.
		 *
		 * @param url an {@code http} or {@code https} URL (must not be {@code null})
		 * @return this builder
		 * @throws IllegalArgumentException if the URL is malformed or uses a non-http(s) protocol
		 */
		public Builder gatewayUrl(String url) {
			Objects.requireNonNull(url, "url");
			try {
				return gatewayUrl(new URL(url));
			} catch (MalformedURLException e) {
				throw new IllegalArgumentException("Invalid gateway URL: " + url, e);
			}
		}

		/**
		 * Validates the configuration and builds the {@link HiggsNode}.
		 *
		 * @return the configured node
		 * @throws IllegalStateException if no authentication mode is configured (neither
		 *         {@code userKey} nor {@code deviceKey}), if {@code deviceKey} is used without a
		 *         {@code userId}, or if any of {@code gatewayNodeId}, {@code gatewayPeerId} or
		 *         {@code gatewayUrl} is missing
		 */
		public HiggsNode build() {
			if (userKey == null && deviceKey == null)
				throw new IllegalStateException("Either userKey (user mode) or deviceKey (device mode) must be set");

			if (userKey == null && userId == null)
				throw new IllegalStateException("userId is required in device mode (when userKey is not set)");

			if (gatewayNodeId == null)
				throw new IllegalStateException("Gateway node ID not set");

			if (gatewayPeerId == null)
				throw new IllegalStateException("Gateway peer ID not set");

			if (gatewayUrl == null)
				throw new IllegalStateException("Gateway URL not set");

			if (userKey != null)
				return new HiggsNode(vertx, userKey, gatewayNodeId, gatewayPeerId, gatewayUrl);
			else
				return new HiggsNode(vertx, userId, deviceKey, gatewayNodeId, gatewayPeerId, gatewayUrl);
		}
	}
}