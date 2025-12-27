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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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
import io.bosonnetwork.crypto.Random;
import io.bosonnetwork.crypto.Signature;
import io.bosonnetwork.utils.Base58;
import io.bosonnetwork.utils.Hex;
import io.bosonnetwork.utils.Json;
import io.bosonnetwork.vertx.BosonVerticle;
import io.bosonnetwork.vertx.VertxFuture;

public class HiggsNode extends BosonVerticle implements Node {
	public static final int PERIODIC_CHECK_INTERVAL = 5 * 60 * 1000;

	private static final String VERSION = "Higgs/1";

	private final Vertx vertxInstance;

	private final Identity identity;

	// user identity
	private final Identity userIdentity;
	// or user id and device identity
	private final Id userId;
	private final Identity deviceIdentity;

	private final Id gatewayNodeId;
	private final URL gatewayUrl;

	private LookupOption defaultLookupOption;

	private String gatewayVersion;
	private WebClient webClient;

	private long periodicCheckTimer;

	private volatile boolean running;

	private final Map<Id, LocalData<Value>> values;
	private final Map<Id, LocalData<PeerInfo>> peers;

	private static final Logger log = LoggerFactory.getLogger(HiggsNode.class);

	private HiggsNode(Vertx vertx, Signature.KeyPair userKey, Id gatewayNodeId, URL gatewayUrl) {
		this.vertxInstance = vertx;
		this.userIdentity = new CryptoIdentity(userKey);
		this.userId = userIdentity.getId();
		this.deviceIdentity = null;
		this.gatewayNodeId = gatewayNodeId;
		this.gatewayUrl = gatewayUrl;
		this.defaultLookupOption = LookupOption.ARBITRARY;
		this.values = new HashMap<>();
		this.peers = new HashMap<>();

		this.identity = userIdentity;
	}

	private HiggsNode(Vertx vertx, Id userId, Signature.KeyPair deviceKey, Id gatewayNodeId, URL gatewayUrl) {
		this.vertxInstance = vertx;
		this.userIdentity = null;
		this.userId = userId;
		this.deviceIdentity = new CryptoIdentity(deviceKey);
		this.gatewayNodeId = gatewayNodeId;
		this.gatewayUrl = gatewayUrl;
		this.defaultLookupOption = LookupOption.ARBITRARY;
		this.values = new HashMap<>();
		this.peers = new HashMap<>();

		this.identity = deviceIdentity;
	}

	@Override
	public Id getId() {
		return identity.getId();
	}

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

	@Override
	public void addConnectionStatusListener(ConnectionStatusListener listener) {
	}

	@Override
	public void removeConnectionStatusListener(ConnectionStatusListener listener) {
	}

	@Override
	public VertxFuture<Void> start() {
		if (this.vertx != null)
			return VertxFuture.failedFuture(new IllegalStateException("Already started"));

		Vertx instance = vertxInstance != null ? vertxInstance : Vertx.vertx();
		Future<Void> future = instance.deployVerticle(this).mapEmpty();
		return VertxFuture.of(future);
	}

	@Override
	public VertxFuture<Void> stop() {
		if (!isRunning())
			return VertxFuture.failedFuture(new IllegalStateException("Not started"));

		Promise<Void> promise = Promise.promise();
		runOnContext(v -> {
			String deploymentId = vertxContext.deploymentID();
			if (deploymentId == null)
				promise.fail(new IllegalStateException("Not started"));

			vertx.undeploy(deploymentId).onComplete(promise);
		});

		return VertxFuture.of(promise.future());
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	@Override
	public VertxFuture<Void> bootstrap(NodeInfo node) {
		return VertxFuture.succeededFuture();
	}

	@Override
	public VertxFuture<Void> bootstrap(Collection<NodeInfo> bootstrapNodes) {
		return VertxFuture.succeededFuture();
	}

	@Override
	public VertxFuture<Result<NodeInfo>> findNode(Id id) {
		return findNode(id, defaultLookupOption);
	}

	@Override
	public VertxFuture<Result<NodeInfo>> findNode(Id id, LookupOption option) {
		Objects.requireNonNull(id, "id");
		if (!isRunning())
			return VertxFuture.failedFuture(new IllegalStateException("Node not running"));

		final LookupOption lookupOption = option != null ? option : defaultLookupOption;
		Promise<Result<NodeInfo>> promise = Promise.promise();

		// noinspection CodeBlock2Expr
		runOnContext(v -> {
			webClient.get("/nodes/" + id)
					.addQueryParam("mode", lookupOption.name().toLowerCase())
					.bearerTokenAuthentication(getAccessToken())
					.send()
					.onSuccess(res -> {
						if (res.statusCode() == 200) {
							JsonObject body = res.bodyAsJsonObject();
							NodeInfo n4 = body.containsKey(Network.IPv4.name()) ?
									body.getJsonObject(Network.IPv4.name()).mapTo(NodeInfo.class) : null;
							NodeInfo n6 = body.containsKey(Network.IPv6.name()) ?
									body.getJsonObject(Network.IPv6.name()).mapTo(NodeInfo.class) : null;
							promise.complete(Result.of(n4, n6));
						} else if (res.statusCode() == 404) {
							promise.complete(null);
						} else {
							promise.fail(wrapErrorResponseToException(res));
						}
					})
					.onFailure((e) -> {
						log.error("Gateway request failed: {}", e.getMessage(), e);
						promise.fail(new HiggsException("Gateway request failed", e));
					});
		});

		return VertxFuture.of(promise.future());
	}

	@Override
	public VertxFuture<Value> findValue(Id id) {
		return findValue(id, -1, null);
	}

	@Override
	public VertxFuture<Value> findValue(Id id, int expectedSequenceNumber) {
		return findValue(id, expectedSequenceNumber, null);
	}

	@Override
	public VertxFuture<Value> findValue(Id id, LookupOption option) {
		return findValue(id, -1, option);
	}

	@Override
	public VertxFuture<Value> findValue(Id id, int expectedSequenceNumber, LookupOption option) {
		Objects.requireNonNull(id, "id");
		if (!isRunning())
			return VertxFuture.failedFuture(new IllegalStateException("Node not running"));

		final LookupOption lookupOption = option != null ? option : defaultLookupOption;
		Promise<Value> promise = Promise.promise();

		runOnContext(v -> {
			HttpRequest<Buffer> request = webClient.get("/values/" + id);
			request.addQueryParam("mode", lookupOption.name().toLowerCase());
			if (expectedSequenceNumber >= 0)
				request.addQueryParam("seq", String.valueOf(expectedSequenceNumber));

			request.bearerTokenAuthentication(getAccessToken())
					.send()
					.onSuccess(res -> {
						if (res.statusCode() == 200) {
							JsonObject body = res.bodyAsJsonObject();
							promise.complete(body.mapTo(Value.class));
						} else if (res.statusCode() == 404) {
							promise.complete(null);
						} else {
							promise.fail(wrapErrorResponseToException(res));
						}
					})
					.onFailure((e) -> {
						log.error("Gateway request failed: {}", e.getMessage(), e);
						promise.fail(new HiggsException("Gateway request failed", e));
					});
		});

		return VertxFuture.of(promise.future());
	}

	@Override
	public VertxFuture<Void> storeValue(Value value) {
		return storeValue(value, -1, false);
	}

	@Override
	public VertxFuture<Void> storeValue(Value value, int expectedSequenceNumber) {
		return storeValue(value, expectedSequenceNumber, false);
	}

	@Override
	public VertxFuture<Void> storeValue(Value value, boolean persistent) {
		return storeValue(value, -1, persistent);
	}

	@Override
	public VertxFuture<Void> storeValue(Value value, int expectedSequenceNumber, boolean persistent) {
		Objects.requireNonNull(value, "value");
		if (!isRunning())
			return VertxFuture.failedFuture(new IllegalStateException("Node not running"));

		JsonObject body = new JsonObject();
		if (expectedSequenceNumber >= 0)
			body.put("expectedSequenceNumber", expectedSequenceNumber);
		body.put("value", value);

		Promise<Void> promise = Promise.promise();

		// noinspection CodeBlock2Expr
		runOnContext(v -> {
			webClient.post("/values")
					.bearerTokenAuthentication(getAccessToken())
					.sendJsonObject(body)
					.onSuccess((res) -> {
						if (res.statusCode() == 201) {
							values.put(value.getId(), new LocalData<>(value, persistent));
							promise.complete();
						} else {
							promise.fail(wrapErrorResponseToException(res));
						}
					})
					.onFailure((e) -> {
						log.error("Gateway request failed: {}", e.getMessage(), e);
						promise.fail(new HiggsException("Gateway request failed", e));
					});
		});

		return VertxFuture.of(promise.future());
	}

	@Override
	public VertxFuture<List<PeerInfo>> findPeer(Id id) {
		return findPeer(id, 0, null);
	}

	@Override
	public VertxFuture<List<PeerInfo>> findPeer(Id id, int expected) {
		return findPeer(id, expected, null);
	}

	@Override
	public VertxFuture<List<PeerInfo>> findPeer(Id id, LookupOption option) {
		return findPeer(id, 0, option);
	}

	@Override
	public VertxFuture<List<PeerInfo>> findPeer(Id id, int expected, LookupOption option) {
		Objects.requireNonNull(id, "id");
		if (expected < 0)
			throw new IllegalArgumentException("expected must be >= 0");

		if (!isRunning())
			return VertxFuture.failedFuture(new IllegalStateException("Node not running"));

		final LookupOption lookupOption = option != null ? option : defaultLookupOption;

		Promise<List<PeerInfo>> promise = Promise.promise();

		runOnContext(v -> {
			HttpRequest<Buffer> request = webClient.get("/peers/" + id);
			request.addQueryParam("mode", lookupOption.name().toLowerCase());
			if (expected > 0)
				request.addQueryParam("expected", Integer.toString(expected));

			request.bearerTokenAuthentication(getAccessToken())
					.send()
					.onSuccess((res) -> {
						if (res.statusCode() == 200) {
								JsonArray body = res.bodyAsJsonArray();
								List<PeerInfo> result = body.stream()
										.map(o -> {
											if (o instanceof JsonArray ja) {
												return Json.objectMapper().convertValue(ja.getList(), PeerInfo.class);
											} else {
												throw new CompletionException(new HiggsException(0, "Gateway error: invalid response"));
											}
										}).toList();
								promise.complete(result);
						} else if (res.statusCode() == 404) {
							promise.complete(List.of());
						} else {
							promise.fail(wrapErrorResponseToException(res));
						}
					})
					.onFailure((e) -> {
						log.error("Gateway request failed: {}", e.getMessage(), e);
						promise.fail(new HiggsException("Gateway request failed", e));
					});
		});

		return VertxFuture.of(promise.future());
	}

	@Override
	public VertxFuture<Void> announcePeer(PeerInfo peer) {
		return announcePeer(peer, false);
	}

	@Override
	public VertxFuture<Void> announcePeer(PeerInfo peer, boolean persistent) {
		Objects.requireNonNull(peer, "peer");
		if (!isRunning())
			return VertxFuture.failedFuture(new IllegalStateException("Node not running"));

		Promise<Void> promise = Promise.promise();

		// noinspection CodeBlock2Expr
		runOnContext(v -> {
			webClient.post("/peers")
					.bearerTokenAuthentication(getAccessToken())
					.sendJson(peer)
					.onSuccess((res) -> {
						if (res.statusCode() == 201) {
							peers.put(peer.getId(), new LocalData<>(peer, persistent));
							promise.complete();
						} else {
							promise.fail(wrapErrorResponseToException(res));
						}
					})
					.onFailure((e) -> {
						log.error("Gateway request failed: {}", e.getMessage(), e);
						promise.fail(new HiggsException("Gateway request failed", e));
					});
		});

		return VertxFuture.of(promise.future());
	}

	@Override
	public VertxFuture<Value> getValue(Id valueId) {
		Objects.requireNonNull(valueId, "valueId");
		Promise<Value> promise = Promise.promise();
		runOnContext(v -> {
			LocalData<Value> data = values.get(valueId);
			promise.complete(data != null ? data.data() : null);
		});
		return VertxFuture.of(promise.future());
	}

	@Override
	public VertxFuture<Boolean> removeValue(Id valueId) {
		Objects.requireNonNull(valueId, "valueId");
		Promise<Boolean> promise = Promise.promise();
		runOnContext(v -> promise.complete(values.remove(valueId) != null));
		return VertxFuture.of(promise.future());
	}

	@Override
	public VertxFuture<PeerInfo> getPeer(Id peerId) {
		Objects.requireNonNull(peerId);
		Promise<PeerInfo> promise = Promise.promise();
		runOnContext(v -> {
			LocalData<PeerInfo> data = peers.get(peerId);
			promise.complete(data != null ? data.data() : null);
		});
		return VertxFuture.of(promise.future());
	}

	@Override
	public VertxFuture<Boolean> removePeer(Id peerId) {
		Objects.requireNonNull(peerId, "peerId");
		Promise<Boolean> promise = Promise.promise();
		runOnContext(v -> promise.complete(peers.remove(peerId) != null));
		return VertxFuture.of(promise.future());
	}

	@Override
	public byte[] sign(byte[] data) {
		Objects.requireNonNull(data, "data");
		return identity.sign(data);
	}

	@Override
	public boolean verify(byte[] data, byte[] signature) {
		Objects.requireNonNull(data, "data");
		Objects.requireNonNull(signature, "data");
		return identity.verify(data, signature);
	}

	@Override
	public byte[] encrypt(Id recipient, byte[] data) throws CryptoException {
		Objects.requireNonNull(recipient, "sender");
		Objects.requireNonNull(data, "data");
		return identity.encrypt(recipient, data);
	}

	@Override
	public byte[] decrypt(Id sender, byte[] data) throws CryptoException {
		Objects.requireNonNull(sender, "sender");
		Objects.requireNonNull(data, "data");
		return identity.decrypt(sender, data);
	}

	@Override
	public CryptoContext createCryptoContext(Id id) throws CryptoException {
		Objects.requireNonNull(id, "id");
		return identity.createCryptoContext(id);
	}

	private Future<String> getGatewayVersion() {
		return webClient.get("/version")
				.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200) {
						String version = res.bodyAsJsonObject().getString("version");
						return Future.succeededFuture(version);
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				});
	}

	private void periodicCheck(long timerId) {
		reannouncePeers();
		reannounceValues();
	}

	private void reannouncePeers() {
		log.info("Trying to reannounce the persistent peers...");
		long ts = System.currentTimeMillis() - MAX_PEER_AGE + PERIODIC_CHECK_INTERVAL * 2;
		List<PeerInfo> todos = peers.values().stream()
				.filter(ld -> ld.isPersistent() && ld.lastAnnounced() <= ts)
				.map(LocalData::data)
				.toList();

		if (todos.isEmpty())
			return;

		Future<Void> chain = Future.succeededFuture();
		for (PeerInfo peer : todos) {
			chain.compose(v -> {
				log.debug("Reannounce the peers {}...", peer.getId());
				return announcePeer(peer, true).toVertxFuture();
			});
		}
	}

	private void reannounceValues() {
		log.info("Trying to reannounce the persistent values...");
		long ts = System.currentTimeMillis() - MAX_VALUE_AGE + PERIODIC_CHECK_INTERVAL * 2;
		List<Value> peers = values.values().stream()
				.filter(ld -> ld.isPersistent() && ld.lastAnnounced() <= ts)
				.map(LocalData::data)
				.toList();

		if (peers.isEmpty())
			return;

		Future<Void> chain = Future.succeededFuture();
		for (Value value : peers) {
			chain.compose(v -> {
				log.debug("Re-announce the value {}...", value.getId());
				return storeValue(value, true).toVertxFuture();
			});
		}
	}

	@Override
	public void prepare(Vertx vertx, Context context) {
		super.prepare(vertx, context);

		WebClientOptions options = new WebClientOptions()
				.setSsl(gatewayUrl.getProtocol().equals("https"))
				.setDefaultHost(gatewayUrl.getHost())
				.setDefaultPort(gatewayUrl.getPort() > 0 ? gatewayUrl.getPort() : gatewayUrl.getDefaultPort())
				.setProtocolVersion(HttpVersion.HTTP_1_1);

		webClient = WebClient.create(vertx, options);
	}

	@Override
	public Future<Void> deploy() {
		return getGatewayVersion().andThen(ar -> {
			if (ar.succeeded()) {
				this.gatewayVersion = ar.result();
				this.periodicCheckTimer = vertx.setPeriodic(60000, PERIODIC_CHECK_INTERVAL, this::periodicCheck);
				running = true;
			} else {
				webClient.close();
				webClient = null;
			}
		}).mapEmpty();
	}

	@Override
	public Future<Void> undeploy() {
		running = false;

		if (periodicCheckTimer > 0) {
			vertx.cancelTimer(periodicCheckTimer);
			periodicCheckTimer = -1;
		}

		if (webClient != null) {
			webClient.close();
			webClient = null;
		}

		return Future.succeededFuture();
	}

	private String getAccessToken() {
		byte[] nonce = Random.randomBytes(24);
		long expiration = System.currentTimeMillis() / 1000 + 600;

		Map<String, Object> claims = new LinkedHashMap<>();
		claims.put("jti", nonce);
		claims.put("iss", identity.getId().bytes());
		claims.put("aud", gatewayNodeId.bytes());
		claims.put("sub", userId.bytes());
		if (deviceIdentity != null)
			claims.put("asc", deviceIdentity.getId().bytes());
		claims.put("exp", expiration);
		claims.put("scp", "client");

		try {
			byte[] payload = Json.cborMapper().writeValueAsBytes(claims);
			byte[] signature = identity.sign(payload);

			return Json.BASE64_ENCODER.encodeToString(payload) + '.' + Json.BASE64_ENCODER.encodeToString(signature);
		} catch (Exception e) {
			throw new RuntimeException("INTERNAL ERROR: Failed to generate the access token", e);
		}
	}

	private HiggsException wrapErrorResponseToException(HttpResponse<Buffer> res) {
		String body = res.bodyAsString();
		if (res.statusCode() == 401) {
			// noinspection LoggingSimilarMessage
			log.error("HTTP status: {}, Unauthorized. {}", res.statusCode(), body);
			return new HiggsException(res.statusCode(), body);
		} else if (res.statusCode() == 429) {
			// noinspection LoggingSimilarMessage
			log.error("HTTP status: {}, Too Many Requests. {}", res.statusCode(), body);
			return new HiggsException(res.statusCode(), body);
		} else {
			// noinspection LoggingSimilarMessage
			log.error("HTTP status: {}, {}", res.statusCode(), body);
			return new HiggsException(res.statusCode(), body);
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {
		private Vertx vertx;
		// user key
		private Signature.KeyPair userKey;
		// or user id and device key
		private Id userId;
		private Signature.KeyPair deviceKey;
		// gateway id and url
		private Id gatewayNodeId;
		private URL gatewayUrl;

		private Builder() {
		}

		public Builder vertx(Vertx vertx) {
			Objects.requireNonNull(vertx, "vertx");
			this.vertx = vertx;
			return this;
		}

		public Builder userKey(Signature.KeyPair key) {
			Objects.requireNonNull(key, "key");
			this.userKey = key;
			return this;
		}

		public Builder userKey(String privateKey) {
			Objects.requireNonNull(privateKey, "privateKey");
			byte[] sk = privateKey.startsWith("0x") ? Hex.decode(privateKey.substring(2)) : Base58.decode(privateKey);
			return userKey(Signature.KeyPair.fromPrivateKey(sk));
		}

		public Builder userKey(byte[] privateKey) {
			Objects.requireNonNull(privateKey, "privateKey");
			if (privateKey.length != Signature.PrivateKey.BYTES)
				throw new IllegalArgumentException("Invalid private key");

			return userKey(Signature.KeyPair.fromPrivateKey(privateKey));
		}

		public Builder userId(Id userId) {
			Objects.requireNonNull(userId, "userId");
			this.userId = userId;
			return this;
		}

		public Builder deviceKey(Signature.KeyPair key) {
			Objects.requireNonNull(key, "key");
			this.deviceKey = key;
			return this;
		}

		public Builder deviceKey(String privateKey) {
			Objects.requireNonNull(privateKey, "privateKey");
			byte[] sk = privateKey.startsWith("0x") ? Hex.decode(privateKey.substring(2)) : Base58.decode(privateKey);
			return deviceKey(Signature.KeyPair.fromPrivateKey(sk));
		}

		public Builder deviceKey(byte[] privateKey) {
			Objects.requireNonNull(privateKey, "privateKey");
			if (privateKey.length != Signature.PrivateKey.BYTES)
				throw new IllegalArgumentException("Invalid private key");

			return deviceKey(Signature.KeyPair.fromPrivateKey(privateKey));
		}

		public Builder gatewayNodeId(Id id) {
			Objects.requireNonNull(id, "gatewayNodeId");
			this.gatewayNodeId = id;
			return this;
		}

		public Builder gatewayUrl(URL url) {
			Objects.requireNonNull(url, "url");
			this.gatewayUrl = url;
			return this;
		}

		public Builder gatewayUrl(String url) {
			Objects.requireNonNull(url, "url");
			try {
				return gatewayUrl(new URL(url));
			} catch (MalformedURLException e) {
				throw new IllegalArgumentException("invalid gateway url: " + url, e);
			}
		}

		public HiggsNode build() {
			if (userKey == null && deviceKey == null)
				throw new IllegalStateException("userKey or deviceKey must be set");

			if (userKey == null && userId == null)
				throw new IllegalStateException("userKey or userId must be set");

			if (gatewayNodeId == null)
				throw new IllegalStateException("gateway node ID not set");

			if (gatewayUrl == null)
				throw new IllegalStateException("gateway URL not set");

			if (userKey != null)
				return new HiggsNode(vertx, userKey, gatewayNodeId, gatewayUrl);
			else
				return new HiggsNode(vertx, userId, deviceKey, gatewayNodeId, gatewayUrl);
		}
	}
}