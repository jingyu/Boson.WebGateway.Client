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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionException;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
import io.bosonnetwork.crypto.Random;
import io.bosonnetwork.crypto.Signature;
import io.bosonnetwork.json.Json;
import io.bosonnetwork.utils.Base58;
import io.bosonnetwork.utils.Hex;
import io.bosonnetwork.utils.Variable;
import io.bosonnetwork.vertx.BosonVerticle;
import io.bosonnetwork.vertx.VertxFuture;

public class HiggsNode extends BosonVerticle implements Node {
	public static final int PERIODIC_CHECK_INTERVAL = 5 * 60 * 1000;
	private static final long ACCESS_TOKEN_TIMEOUT = 10 * 60 * 1000;

	private static final String VERSION = "Higgs/1";

	private final Vertx vertxInstance;

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

	private String accessToken;
	private long accessTokenCreatedTime;

	private long periodicCheckTimer;

	private volatile boolean running;

	private final Map<Id, LocalData<Value>> values;
	private final Map<Id, List<LocalData<PeerInfo>>> peers;

	private static final Logger log = LoggerFactory.getLogger(HiggsNode.class);

	private HiggsNode(Vertx vertx, Signature.KeyPair userKey, Id gatewayNodeId, Id gatewayPeerId, URL gatewayUrl) {
		this.vertxInstance = vertx;
		this.userIdentity = new CryptoIdentity(userKey);
		this.userId = userIdentity.getId();
		this.deviceIdentity = null;
		this.gatewayNodeId = gatewayNodeId;
		this.gatewayPeerId = gatewayPeerId;
		this.gatewayUrl = gatewayUrl;
		this.defaultLookupOption = LookupOption.ARBITRARY;
		this.values = new HashMap<>();
		this.peers = new HashMap<>();

		this.identity = userIdentity;
	}

	private HiggsNode(Vertx vertx, Id userId, Signature.KeyPair deviceKey, Id gatewayNodeId, Id gatewayPeerId, URL gatewayUrl) {
		this.vertxInstance = vertx;
		this.userIdentity = null;
		this.userId = userId;
		this.deviceIdentity = new CryptoIdentity(deviceKey);
		this.gatewayNodeId = gatewayNodeId;
		this.gatewayPeerId = gatewayPeerId;
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
							if (n4 == n6) // all null
								promise.complete(null);
							else
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
	public VertxFuture<Value> findValue(Id id, int expectedSequenceNumber, LookupOption option) {
		Objects.requireNonNull(id, "id");
		if (expectedSequenceNumber < -1)
			throw new IllegalArgumentException("expectedSequenceNumber must be >= -1");
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
	public VertxFuture<Void> storeValue(Value value, int expectedSequenceNumber, boolean persistent) {
		Objects.requireNonNull(value, "value");
		if (expectedSequenceNumber < -1)
			throw new IllegalArgumentException("expectedSequenceNumber must be >= -1");
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
	public VertxFuture<List<PeerInfo>> findPeer(Id id, int expectedSequenceNumber, int expectedCount, LookupOption option) {
		Objects.requireNonNull(id, "id");
		if (expectedSequenceNumber < -1)
			throw new IllegalArgumentException("expectedSequenceNumber must be >= -1");
		if (expectedCount < 0)
			throw new IllegalArgumentException("expectedCount must be >= 0");

		if (!isRunning())
			return VertxFuture.failedFuture(new IllegalStateException("Node not running"));

		final LookupOption lookupOption = option != null ? option : defaultLookupOption;

		Promise<List<PeerInfo>> promise = Promise.promise();

		runOnContext(v -> {
			HttpRequest<Buffer> request = webClient.get("/peers/" + id);
			request.addQueryParam("mode", lookupOption.name().toLowerCase());
			if (expectedSequenceNumber >= 0)
				request.addQueryParam("seq", Integer.toString(expectedSequenceNumber));
			request.addQueryParam("count", Integer.toString(expectedCount));

			request.bearerTokenAuthentication(getAccessToken())
					.send()
					.onSuccess((res) -> {
						if (res.statusCode() == 200) {
								JsonArray body = res.bodyAsJsonArray();
								List<PeerInfo> result = body.stream()
										.map(o -> {
											if (o instanceof JsonObject jo) {
												return Json.objectMapper().convertValue(jo.getMap(), PeerInfo.class);
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
	public VertxFuture<Void> announcePeer(PeerInfo peer, int expectedSequenceNumber, boolean persistent) {
		Objects.requireNonNull(peer, "peer");
		if (expectedSequenceNumber < -1)
			throw new IllegalArgumentException("expectedSequenceNumber must be >= -1");
		if (!isRunning())
			return VertxFuture.failedFuture(new IllegalStateException("Node not running"));

		JsonObject body = new JsonObject();
		if (expectedSequenceNumber >= 0)
			body.put("expectedSequenceNumber", expectedSequenceNumber);
		body.put("peer", peer);

		Promise<Void> promise = Promise.promise();

		// noinspection CodeBlock2Expr
		runOnContext(v -> {
			PeerInfo existing = _getPeer(peer.getId(), peer.getFingerprint());
			if (existing != null && existing.getSequenceNumber() > peer.getSequenceNumber())
				promise.fail(new HiggsException(0, "Peer already announced with a higher sequence number"));

			webClient.post("/peers")
					.bearerTokenAuthentication(getAccessToken())
					.sendJson(body)
					.onSuccess((res) -> {
						if (res.statusCode() == 201) {
							_putPeer(peer, persistent);
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

	/**
	 * Compares two peers to determine their ordering priority.
	 * Ordering rules:
	 * 1. Sequence number in descending order (higher first).
	 * 2. Authentication status (authenticated peers before unauthenticated).
	 * 3. For authenticated peers, XOR distance to the target is used as a tiebreaker.
	 * <p>
	 * Unauthenticated peers compare as equal and are thus unordered relative to each other.
	 *
	 * @param p1 the first peer to compare
	 * @param p2 the second peer to compare
	 * @return negative if p1 < p2, positive if p1 > p2, zero if equal
	 */
	private static int peerOrder(PeerInfo p1, PeerInfo p2) {
		int diff = Integer.compare(p2.getSequenceNumber(), p1.getSequenceNumber());
		if (diff != 0)
			return diff;

		diff = Boolean.compare(p2.isAuthenticated(), p1.isAuthenticated());
		if (diff != 0)
			return diff;

		// Kademlia XOR distance
		if (p1.isAuthenticated() && p2.isAuthenticated())
			return p1.getId().threeWayCompare(p1.getNodeId(), p2.getNodeId());

		return 0;
	}

	@Override
	public VertxFuture<List<PeerInfo>> getPeers(Id peerId) {
		Promise<List<PeerInfo>> promise = Promise.promise();
		runOnContext(v -> {
			List<PeerInfo> result = peers.getOrDefault(peerId, List.of()).stream()
					.map(LocalData::data)
					.sorted(HiggsNode::peerOrder)
					.toList();
			promise.complete(result);
		});
		return VertxFuture.of(promise.future());
	}

	@Override
	public VertxFuture<Boolean> removePeers(Id peerId) {
		Promise<Boolean> promise = Promise.promise();
		runOnContext(v -> promise.complete(peers.remove(peerId) != null));
		return VertxFuture.of(promise.future());
	}

	@Override
	public VertxFuture<PeerInfo> getPeer(Id peerId, long fingerprint) {
		Promise<PeerInfo> promise = Promise.promise();
		runOnContext(v -> promise.complete(_getPeer(peerId, fingerprint)));
		return VertxFuture.of(promise.future());
	}

	@Override
	public VertxFuture<Boolean> removePeer(Id peerId, long fingerprint) {
		Promise<Boolean> promise = Promise.promise();
		runOnContext(v -> promise.complete(_removePeer(peerId, fingerprint)));
		return VertxFuture.of(promise.future());
	}

	private PeerInfo _getPeer(Id peerId, long fingerprint) {
		List<LocalData<PeerInfo>> lds = peers.getOrDefault(peerId, List.of());
		for (LocalData<PeerInfo> ld : lds) {
			if (ld.data().getFingerprint() == fingerprint)
				return ld.data();
		}
		return null;
	}

	private boolean _putPeer(PeerInfo peer, boolean persistent) {
		Variable<Boolean> updated = Variable.of(false);

		peers.compute(peer.getId(), (k, v) -> {
			if (v == null) {
				updated.set(true);
				return List.of(new LocalData<>(peer, persistent));
			}

			if (v.size() == 1) {
				if (v.get(0).data().getFingerprint() == peer.getFingerprint()) {
					// same peer
					if (v.get(0).data().getSequenceNumber() <= peer.getSequenceNumber()) {
						updated.set(true);
						return List.of(new LocalData<>(peer, persistent));
					} else {
						return v;
					}
				} else {
					List<LocalData<PeerInfo>> result = new ArrayList<>();
					result.add(v.get(0));
					result.add(new LocalData<>(peer, persistent));
					updated.set(true);
					return result;
				}
			}

			for (int i = 0; i < v.size(); i++) {
				if (v.get(i).data().getFingerprint() == peer.getFingerprint()) {
					// same peer
					if (v.get(i).data().getSequenceNumber() <= peer.getSequenceNumber()) {
						v.set(i, new LocalData<>(peer, persistent));
						updated.set(true);
						return v;
					} else {
						return v;
					}
				}
			}

			v.add(new LocalData<>(peer, persistent));
			updated.set(true);
			return v;
		});

		return updated.get();
	}

	private boolean _removePeer(Id peerId, long fingerprint) {
		Variable<Boolean> removed = Variable.of(false);

		peers.compute(peerId, (k, v) -> {
			if (v == null) {
				removed.set(false);
				return null;
			}

			if (v.size() == 1) { // optimized, Immutable map
				if (v.get(0).data().getFingerprint() == fingerprint) {
					removed.set(true);
					return null;
				} else {
					return v;
				}
			}

			final Iterator<LocalData<PeerInfo>> it = v.iterator();
			while (it.hasNext()) {
				LocalData<PeerInfo> ld = it.next();
				if (ld.data().getFingerprint() == fingerprint) {
					it.remove();
					removed.set(true);
					break;
				}
			}

			if (removed.get())
				return v.isEmpty() ? null : (v.size() == 1 ? List.of(v.get(0)) : v);
			else
				return v;
		});

		return removed.get();
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
	public byte[] encrypt(Id receiver, byte[] nonce, byte[] data) throws CryptoException {
		Objects.requireNonNull(receiver, "receiver");
		Objects.requireNonNull(nonce, "nonce");
		Objects.requireNonNull(data, "data");
		return identity.encrypt(receiver, nonce, data);
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
		evictExpiredData();
		reannouncePeers();
		reannounceValues();
	}

	private void evictExpiredData() {
		values.entrySet().removeIf(entry -> entry.getValue().isExpired());

		for (Iterator<Map.Entry<Id, List<LocalData<PeerInfo>>>> it = peers.entrySet().iterator(); it.hasNext(); ) {
			Map.Entry<Id, List<LocalData<PeerInfo>>> entry = it.next();
			List<LocalData<PeerInfo>> lst = entry.getValue();
			if (lst.isEmpty()) {
				it.remove();
				continue;
			}

			if (lst.size() == 1) {
				if (lst.get(0).isExpired())
					it.remove();

				continue;
			}

			lst.removeIf(LocalData::isExpired);
			if (lst.isEmpty()) {
				it.remove();
				continue;
			}

			if (lst.size() == 1)
				entry.setValue(List.of(lst.get(0)));
		}
	}

	private void reannouncePeers() {
		log.info("Trying to reannounce the persistent peers...");
		long ts = System.currentTimeMillis() - MAX_PEER_AGE + PERIODIC_CHECK_INTERVAL * 2;
		List<PeerInfo> todos = peers.values().stream()
				.flatMap(Collection::stream)
				.filter(ld -> ld.isPersistent() && ld.lastAnnounced() <= ts)
				.map(LocalData::data)
				.toList();

		if (todos.isEmpty())
			return;

		Future<Void> chain = Future.succeededFuture();
		for (PeerInfo peer : todos) {
			chain.compose(v -> {
				log.debug("Reannounce the peers {}...", peer.getId());
				return Future.fromCompletionStage(announcePeer(peer, true));
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
				return Future.fromCompletionStage(storeValue(value, true));
			});
		}
	}

	@Override
	protected void prepare(Vertx vertx, Context context) {
		super.prepare(vertx, context);

		boolean ssl = gatewayUrl.getProtocol().equals("https");
		WebClientOptions options = new WebClientOptions()
				.setSsl(ssl)
				.setDefaultHost(gatewayUrl.getHost())
				.setDefaultPort(gatewayUrl.getPort() > 0 ? gatewayUrl.getPort() : gatewayUrl.getDefaultPort())
				.setProtocolVersion(HttpVersion.HTTP_1_1);

		if (ssl) {
			options.setEnabledSecureTransportProtocols(Set.of("TLSv1.3"))
					.setTrustOptions(TrustOptions.wrap(new HybridTrustManager(gatewayPeerId.toString(), gatewayPeerId.bytes())));
		}

		webClient = WebClient.create(vertx, options);
	}

	@Override
	protected Future<Void> deploy() {
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
	protected Future<Void> undeploy() {
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
		long now = System.currentTimeMillis();
		if ((now - accessTokenCreatedTime) > (ACCESS_TOKEN_TIMEOUT - 60000)) {
			byte[] nonce = Random.randomBytes(24);
			long expiration = (now + ACCESS_TOKEN_TIMEOUT) / 1000;

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

				accessToken = Json.BASE64_ENCODER.encodeToString(payload) + '.' + Json.BASE64_ENCODER.encodeToString(signature);
				accessTokenCreatedTime = now;
			} catch (Exception e) {
				throw new RuntimeException("INTERNAL ERROR: Failed to generate the access token", e);
			}
		}

		return accessToken;
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
		private Id gatewayPeerId;
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

		public Builder gatewayPeerId(Id id) {
			Objects.requireNonNull(id, "gatewayPeerId");
			this.gatewayPeerId = id;
			return this;
		}

		public Builder gatewayUrl(URL url) {
			Objects.requireNonNull(url, "url");
			if (!url.getProtocol().equals("http") && !url.getProtocol().equals("https"))
				throw new IllegalArgumentException("Invalid url protocol: " + url.getProtocol());
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

			if (gatewayPeerId == null)
				throw new IllegalStateException("gateway peer ID not set");

			if (gatewayUrl == null)
				throw new IllegalStateException("gateway URL not set");

			if (userKey != null)
				return new HiggsNode(vertx, userKey, gatewayNodeId, gatewayPeerId, gatewayUrl);
			else
				return new HiggsNode(vertx, userId, deviceKey, gatewayNodeId, gatewayPeerId, gatewayUrl);
		}
	}
}