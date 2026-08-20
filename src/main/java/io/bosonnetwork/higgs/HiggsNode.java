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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.core.type.TypeReference;
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
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.bosonnetwork.ConnectionStatusListener;
import io.bosonnetwork.CryptoContext;
import io.bosonnetwork.AnnounceResult;
import io.bosonnetwork.Id;
import io.bosonnetwork.Identity;
import io.bosonnetwork.LookupOption;
import io.bosonnetwork.Node;
import io.bosonnetwork.NodeInfo;
import io.bosonnetwork.PeerInfo;
import io.bosonnetwork.Value;
import io.bosonnetwork.crypto.CryptoException;
import io.bosonnetwork.crypto.CryptoIdentity;
import io.bosonnetwork.crypto.HybridTrustManager;
import io.bosonnetwork.crypto.Signature;
import io.bosonnetwork.cwt.SignedCwt;
import io.bosonnetwork.higgs.exceptions.GatewayTimeoutException;
import io.bosonnetwork.higgs.exceptions.HiggsException;
import io.bosonnetwork.higgs.exceptions.RateLimitException;
import io.bosonnetwork.higgs.exceptions.ServiceBusyException;
import io.bosonnetwork.higgs.exceptions.UnauthorizedException;
import io.bosonnetwork.json.Json;
import io.bosonnetwork.service.AccessScope;
import io.bosonnetwork.utils.Base58;
import io.bosonnetwork.utils.Hex;
import io.bosonnetwork.vertx.ContextualFuture;
import io.bosonnetwork.web.PaginatedResult;

/**
 * A lightweight {@link Node} implementation backed by a Boson Web Gateway's authenticated
 * HTTP/REST API.
 * <p>
 * Instead of joining the DHT directly, a {@code HiggsNode} forwards {@code Node} operations
 * (find/store value, find/announce peer) to a gateway service, and additionally exposes the
 * gateway's per-user persistent storage ({@link #getValue}, {@link #removeValue},
 * {@link #getPeers}, {@link #getPeer}, {@link #removePeers}, {@link #removePeer}). It keeps no
 * routing table or local storage, so it is inexpensive to create and suited to clients (mobile, CLI,
 * embedded) that cannot run a full node.
 *
 * <h2>Authentication</h2>
 * Every request carries a short-lived {@link io.bosonnetwork.cwt.SignedCwt CWT} bearer token. The
 * token is always signed by the device key ({@link Builder#deviceKey}, required), with the device id
 * set as the token's client id. The acting user is identified by the token subject, configured at
 * build time either directly via {@link Builder#userId} (the device acts on behalf of that user) or
 * derived from a user key pair via {@link Builder#userKey}.
 *
 * <h2>Gateway binding</h2>
 * The target gateway is identified by its node id, peer id and URL ({@link Builder#gatewayNodeId},
 * {@link Builder#gatewayPeerId}, {@link Builder#gatewayUrl}). {@link #start()} fetches the gateway's
 * {@code /info} and verifies the returned node and peer ids match the configured values, failing fast
 * on mismatch; over HTTPS the gateway's self-signed certificate is pinned to its peer id.
 *
 * <h2>Lifecycle &amp; threading</h2>
 * Call {@link #start()} before issuing requests and {@link #stop()} when finished; requests made while
 * not running fail with {@link IllegalStateException}. Built on Vert.x - the returned
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

	// current support API version prefix
	private static final String API_VERSION_PREFIX = "/v1";

	private final Vertx vertx;

	private final Id userId;
	private final Identity deviceIdentity;

	private @Nullable Id gatewayNodeId;
	private final Id gatewayPeerId;
	private final URL gatewayUrl;

	private LookupOption defaultLookupOption;

	private volatile @Nullable String gatewayVersion;
	private @Nullable WebClient webClient;

	private volatile @Nullable AccessTokenCache tokenCache;

	private final AtomicBoolean running;

	private static final Logger log = LoggerFactory.getLogger(HiggsNode.class);

	private record AccessTokenCache(String token, long createdAt) {}

	private HiggsNode(Builder builder) {
		this.vertx = Objects.requireNonNull(builder.vertx, "Vert.x instance must be set");

		this.userId = Objects.requireNonNull(builder.userId, "Either userId or userKey must be set");

		Objects.requireNonNull(builder.deviceKey, "deviceKey must be set");
		this.deviceIdentity = new CryptoIdentity(builder.deviceKey);

		this.gatewayPeerId = Objects.requireNonNull(builder.gatewayPeerId, "gatewayPeerId must be set");
		this.gatewayUrl = Objects.requireNonNull(builder.gatewayUrl, "gatewayUrl must be set");

		this.defaultLookupOption = LookupOption.ARBITRARY;
		this.running = new AtomicBoolean(false);
	}

	public String getGatewayInfo() {
		return gatewayUrl + " @ " + gatewayPeerId +
				(gatewayNodeId != null ? " @ " + gatewayNodeId : "") +
				(gatewayVersion != null ? " : " + gatewayVersion : "N/A");
	}

	@Override
	public Id getId() {
		return deviceIdentity.getId();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @implNote Unsupported by the gateway client: a {@code HiggsNode} is a thin REST client with
	 *           no local DHT node, so it has no {@link NodeInfo} of its own. Always throws
	 *           {@link UnsupportedOperationException}.
	 */
	@Override
	public Optional<NodeInfo> getNodeInfo() {
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

		Json.initializeBosonJsonModule();

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

		Future<Void> future = fetchGatewayInfo().compose(info -> {
			Id peerId = Id.of(info.getString("peerId"));
			if (!peerId.equals(gatewayPeerId))
				return Future.failedFuture(new HiggsException(HiggsException.NO_HTTP_STATUS, "Gateway peer ID mismatch"));

			this.gatewayNodeId = Id.of(info.getString("nodeId"));

			this.gatewayVersion = info.getString("version");
			return Future.succeededFuture();
		}).onFailure(e -> {
			log.error("Failed to get gateway info", e);
			if (webClient != null) {
				webClient.close();
				webClient = null;
			}
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
	public ContextualFuture<Optional<NodeInfo>> findNode(Id id, @Nullable LookupOption option) {
		Objects.requireNonNull(id, "id");
		runningCheck();

		final LookupOption lookupOption = option != null ? option : defaultLookupOption;
		WebClient webClient = requireInitialized(this.webClient, "webClient");

		Future<Optional<NodeInfo>> future = webClient.get(API_VERSION_PREFIX + "/nodes/" + id)
				.addQueryParam("mode", lookupOption.name().toLowerCase())
				.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200) {
						NodeInfo ni = requireBody(res.bodyAsJson(NodeInfo.class));
						return Future.succeededFuture(Optional.of(ni));
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture(Optional.<NodeInfo>empty());
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover(e -> {
					if (e instanceof HiggsException he)
						return Future.failedFuture(he);
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Optional<Value>> findValue(Id id, int expectedSequenceNumber, @Nullable LookupOption option) {
		Objects.requireNonNull(id, "id");
		if (expectedSequenceNumber < -1)
			throw new IllegalArgumentException("expectedSequenceNumber must be >= -1");
		runningCheck();

		final LookupOption lookupOption = option != null ? option : defaultLookupOption;
		WebClient webClient = requireInitialized(this.webClient, "webClient");

		HttpRequest<Buffer> request = webClient.get(API_VERSION_PREFIX + "/values/" + id);
		request.addQueryParam("mode", lookupOption.name().toLowerCase());
		if (expectedSequenceNumber >= 0)
			request.addQueryParam("seq", String.valueOf(expectedSequenceNumber));

		Future<Optional<Value>> future = request.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200) {
						Value value = requireBody(res.bodyAsJson(Value.class));
						return Future.succeededFuture(Optional.of(value));
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture(Optional.<Value>empty());
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover((e) -> {
					if (e instanceof HiggsException he)
						return Future.failedFuture(he);
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<AnnounceResult> storeValue(Value value, int expectedSequenceNumber, boolean persistent) {
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

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		Future<AnnounceResult> future = webClient.post(API_VERSION_PREFIX + "/values")
				.bearerTokenAuthentication(getAccessToken())
				.sendJsonObject(body)
				.compose(res -> {
					if (res.statusCode() == 201) {
						AnnounceResult ar = requireBody(res.bodyAsJson(AnnounceResult.class));
						return Future.succeededFuture(ar);
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover(e -> {
					if (e instanceof HiggsException he)
						return Future.failedFuture(he);
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<List<PeerInfo>> findPeer(Id id, int expectedSequenceNumber, int expectedCount, @Nullable LookupOption option) {
		Objects.requireNonNull(id, "id");
		if (expectedSequenceNumber < -1)
			throw new IllegalArgumentException("expectedSequenceNumber must be >= -1");
		if (expectedCount < 0)
			throw new IllegalArgumentException("expectedCount must be >= 0");
		runningCheck();

		final LookupOption lookupOption = option != null ? option : defaultLookupOption;
		WebClient webClient = requireInitialized(this.webClient, "webClient");

		HttpRequest<Buffer> request = webClient.get(API_VERSION_PREFIX + "/peers/" + id);
		request.addQueryParam("mode", lookupOption.name().toLowerCase());
		if (expectedSequenceNumber >= 0)
			request.addQueryParam("seq", Integer.toString(expectedSequenceNumber));
		request.addQueryParam("count", Integer.toString(expectedCount));

		Future<List<PeerInfo>> future = request.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200) {
						JsonArray body = requireBody(res.bodyAsJsonArray());
						List<PeerInfo> result = Json.objectMapper().convertValue(body.getList(), new TypeReference<>() {});
						return Future.succeededFuture(result);
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture(List.<PeerInfo>of());
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover(e -> {
					if (e instanceof HiggsException he)
						return Future.failedFuture(he);
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(new HiggsException("Gateway request failed", e));
				});


		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<AnnounceResult> announcePeer(PeerInfo peer, int expectedSequenceNumber, boolean persistent) {
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

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		Future<AnnounceResult> future = webClient.post(API_VERSION_PREFIX + "/peers")
				.bearerTokenAuthentication(getAccessToken())
				.sendJsonObject(body)
				.compose(res -> {
					if (res.statusCode() == 201) {
						AnnounceResult ar = requireBody(res.bodyAsJson(AnnounceResult.class));
						return Future.succeededFuture(ar);
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover((e) -> {
					if (e instanceof HiggsException he)
						return Future.failedFuture(he);
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Optional<Value>> getValue(Id valueId) {
		Objects.requireNonNull(valueId, "valueId");
		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		Future<Optional<Value>> future = webClient.get(API_VERSION_PREFIX + "/user/values/" + valueId)
				.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200) {
						Value value = requireBody(res.bodyAsJson(Value.class));
						return Future.succeededFuture(Optional.of(value));
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture(Optional.<Value>empty());
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover((e) -> {
					if (e instanceof HiggsException he)
						return Future.failedFuture(he);
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	public ContextualFuture<PaginatedResult<Value>> getAllValues(long page, long pageSize) {
		if (page <= 0)
			throw new IllegalArgumentException("page must be >= 1");
		if (pageSize <= 0)
			throw new IllegalArgumentException("pageSize must be >= 1");

		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		HttpRequest<Buffer> request = webClient.get(API_VERSION_PREFIX + "/user/values");
		if (page > 1 || pageSize != Long.MAX_VALUE) {
			request.addQueryParam("page", Long.toString(page));
			request.addQueryParam("pageSize", Long.toString(pageSize));
		}

		Future<PaginatedResult<Value>> future = request
				.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200) {
						JsonObject body = requireBody(res.bodyAsJsonObject());
						PaginatedResult<Value> result = Json.objectMapper().convertValue(body.getMap(), new TypeReference<>() {});
						return Future.succeededFuture(result);
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture(PaginatedResult.of(page, pageSize, 0, List.<Value>of()));
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover((e) -> {
					if (e instanceof HiggsException he)
						return Future.failedFuture(he);
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Boolean> removeValue(Id valueId) {
		Objects.requireNonNull(valueId, "valueId");
		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		Future<Boolean> future = webClient.delete(API_VERSION_PREFIX + "/user/values/" + valueId)
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
					if (e instanceof HiggsException he)
						return Future.failedFuture(he);
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<List<PeerInfo>> getPeers(Id peerId) {
		Objects.requireNonNull(peerId, "peerId");
		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		Future<List<PeerInfo>> future = webClient.get(API_VERSION_PREFIX + "/user/peers/" + peerId)
				.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200) {
						JsonArray body = requireBody(res.bodyAsJsonArray());
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
						return Future.succeededFuture(List.<PeerInfo>of());
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover(e -> {
					if (e instanceof HiggsException he)
						return Future.failedFuture(he);
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	public ContextualFuture<PaginatedResult<PeerInfo>> getAllPeers(long page, long pageSize) {
		if (page <= 0)
			throw new IllegalArgumentException("page must be >= 1");
		if (pageSize <= 0)
			throw new IllegalArgumentException("pageSize must be >= 1");

		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		HttpRequest<Buffer> request = webClient.get(API_VERSION_PREFIX + "/user/peers");
		if (page > 1 || pageSize != Long.MAX_VALUE) {
			request.addQueryParam("page", Long.toString(page));
			request.addQueryParam("pageSize", Long.toString(pageSize));
		}

		Future<PaginatedResult<PeerInfo>> future = request
				.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200) {
						JsonObject body = requireBody(res.bodyAsJsonObject());
						PaginatedResult<PeerInfo> result = Json.objectMapper().convertValue(body.getMap(), new TypeReference<>() {});
						return Future.succeededFuture(result);
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture(PaginatedResult.of(page, pageSize, 0, List.<PeerInfo>of()));
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover(e -> {
					if (e instanceof HiggsException he)
						return Future.failedFuture(he);
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Boolean> removePeers(Id peerId) {
		Objects.requireNonNull(peerId, "peerId");
		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		Future<Boolean> future = webClient.delete(API_VERSION_PREFIX + "/user/peers/" + peerId)
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
					if (e instanceof HiggsException he)
						return Future.failedFuture(he);
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Optional<PeerInfo>> getPeer(Id peerId, long fingerprint) {
		Objects.requireNonNull(peerId, "peerId");
		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		Future<Optional<PeerInfo>> future = webClient.get(API_VERSION_PREFIX + "/user/peers/" + peerId + "/" + fingerprint)
				.bearerTokenAuthentication(getAccessToken())
				.send()
				.compose(res -> {
					if (res.statusCode() == 200) {
						PeerInfo pi = requireBody(res.bodyAsJson(PeerInfo.class));
						return Future.succeededFuture(Optional.of(pi));
					} else if (res.statusCode() == 404) {
						return Future.succeededFuture(Optional.<PeerInfo>empty());
					} else {
						return Future.failedFuture(wrapErrorResponseToException(res));
					}
				})
				.recover(e -> {
					if (e instanceof HiggsException he)
						return Future.failedFuture(he);
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public ContextualFuture<Boolean> removePeer(Id peerId, long fingerprint) {
		Objects.requireNonNull(peerId, "peerId");
		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		Future<Boolean> future = webClient.delete(API_VERSION_PREFIX + "/user/peers/" + peerId + "/" + fingerprint)
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
					if (e instanceof HiggsException he)
						return Future.failedFuture(he);
					log.error("Gateway request failed: {}", e.getMessage(), e);
					return Future.failedFuture(new HiggsException("Gateway request failed", e));
				});

		return ContextualFuture.of(future);
	}

	@Override
	public byte[] sign(byte[] data) {
		Objects.requireNonNull(data, "data");
		return deviceIdentity.sign(data);
	}

	@Override
	public boolean verify(byte[] data, byte[] signature) {
		Objects.requireNonNull(data, "data");
		Objects.requireNonNull(signature, "signature");
		return deviceIdentity.verify(data, signature);
	}

	@Override
	public byte[] encrypt(Id recipient, byte[] data) throws CryptoException {
		Objects.requireNonNull(recipient, "recipient");
		Objects.requireNonNull(data, "data");
		return deviceIdentity.encrypt(recipient, data);
	}

	@Override
	public byte[] encrypt(Id recipient, byte[] nonce, byte[] data) throws CryptoException {
		Objects.requireNonNull(recipient, "recipient");
		Objects.requireNonNull(nonce, "nonce");
		Objects.requireNonNull(data, "data");
		return deviceIdentity.encrypt(recipient, nonce, data);
	}

	@Override
	public byte[] decrypt(Id sender, byte[] data) throws CryptoException {
		Objects.requireNonNull(sender, "sender");
		Objects.requireNonNull(data, "data");
		return deviceIdentity.decrypt(sender, data);
	}

	@Override
	public byte[] decrypt(Id sender, byte[] nonce, byte[] data) throws CryptoException {
		Objects.requireNonNull(sender, "sender");
		Objects.requireNonNull(nonce, "nonce");
		Objects.requireNonNull(data, "data");
		return deviceIdentity.decrypt(sender, nonce, data);
	}

	@Override
	public CryptoContext createCryptoContext(Id id) throws CryptoException {
		Objects.requireNonNull(id, "id");
		return deviceIdentity.createCryptoContext(id);
	}

	@Override
	public <T> Optional<T> unwrap(Class<T> clazz) {
		if (clazz.isInstance(vertx))
			return Optional.of(clazz.cast(vertx));

		return Optional.empty();
	}

	private Future<JsonObject> fetchGatewayInfo() {
		WebClient webClient = requireInitialized(this.webClient, "webClient");
		return webClient.get(API_VERSION_PREFIX + "/info")
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
			SignedCwt.Builder builder = SignedCwt.builder(deviceIdentity)
					.subject(userId)
					.audience(gatewayPeerId)
					.expiration(Duration.ofMillis(ACCESS_TOKEN_TIMEOUT + 1000 * 60))
					.notBeforeNow()
					.issuedAtNow()
					.scope(AccessScope.CLIENT.toString())
					.clientId(deviceIdentity.getId());

			String token = builder.buildToString();
			tc = new AccessTokenCache(token, System.currentTimeMillis());
			tokenCache = tc;
		}

		return tc.token;
	}

	private HiggsException wrapErrorResponseToException(HttpResponse<Buffer> res) {
		HiggsException error = HiggsException.fromResponse(res);

		// Conditions the caller is expected to meet in normal operation - an expired token, a
		// throttle, a shed request, a timed-out lookup - are logged at debug. Logging them at error
		// level would flood the log at exactly the moment a client is already backing off, and would
		// train users of this library to ignore the level that real faults use.
		if (error instanceof UnauthorizedException || error instanceof RateLimitException
				|| error instanceof ServiceBusyException || error instanceof GatewayTimeoutException)
			log.debug("HTTP status: {}, {}: {}", error.getStatus(),
					error.getClass().getSimpleName(), error.getMessage());
		else
			log.error("HTTP status: {}, {}: {}", error.getStatus(),
					error.getClass().getSimpleName(), error.getMessage());

		return error;
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
	 * A {@link #deviceKey(Signature.KeyPair) deviceKey} is always required: it signs every token, with
	 * the device id set as the token's client id. The acting user must also be supplied, either
	 * directly via {@link #userId(Id) userId} or derived from a user key pair via
	 * {@link #userKey(Signature.KeyPair) userKey}. The gateway coordinates
	 * {@link #gatewayPeerId(Id) gatewayPeerId} and {@link #gatewayUrl(URL) gatewayUrl} are
	 * all required. {@link #build()} validates the configuration and throws {@link IllegalStateException}
	 * if anything required is missing.
	 * A {@link Vertx} instance may be supplied via {@link #vertx(Vertx)}. Not thread-safe.
	 */
	@NullUnmarked
	public static class Builder {
		private Vertx vertx;
		// user id
		private Id userId;
		// or user key
		@SuppressWarnings("unused")
		private Signature.KeyPair userKey;
		private Signature.KeyPair deviceKey;
		// gateway peer id and url
		private Id gatewayPeerId;
		private URL gatewayUrl;

		private Builder() {
			// Try to autoconfigure the Vert.x instance if the builder is created within a Vert.x context.
			this.vertx = Vertx.currentContext() != null ? Vertx.currentContext().owner() : null;
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
		 * Sets the acting user id directly. The configured {@link #deviceKey} signs tokens on behalf
		 * of this user. Clears any previously set {@link #userKey}.
		 *
		 * @param userId the user id (must not be {@code null})
		 * @return this builder
		 */
		public Builder userId(Id userId) {
			Objects.requireNonNull(userId, "userId");
			this.userId = userId;
			this.userKey = null;
			return this;
		}

		/**
		 * Sets the acting user from a user key pair: the user id is derived from the key's public key.
		 * A convenience alternative to {@link #userId(Id)} when the user key pair is available. Note
		 * that the device key, not this key, signs the tokens.
		 *
		 * @param key the user key pair (must not be {@code null})
		 * @return this builder
		 */
		public Builder userKey(Signature.KeyPair key) {
			Objects.requireNonNull(key, "key");
			this.userKey = key;
			this.userId = Id.of(key.publicKey().bytes());
			return this;
		}

		/**
		 * Sets the acting user from an encoded user private key string (see {@link #userKey(Signature.KeyPair)}).
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
		 * Sets the acting user from a raw user private key (see {@link #userKey(Signature.KeyPair)}).
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
		 * Sets the device key pair (required). The device key signs every token, with the device id
		 * set as the token's client id.
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
		 * Sets the device key from an encoded private key string (see {@link #deviceKey(Signature.KeyPair)}).
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
		 * Sets the device key from a raw private key (see {@link #deviceKey(Signature.KeyPair)}).
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
		 * @throws IllegalStateException if {@code deviceKey} is missing, if neither {@code userId} nor
		 *         {@code userKey} is set, or if any of {@code gatewayPeerId} or {@code gatewayUrl} is missing
		 */
		public HiggsNode build() {
			try {
				return new HiggsNode(this);
			} catch (NullPointerException | IllegalArgumentException e) {
				throw new IllegalStateException("Invalid HiggsNode configuration: " + e.getMessage(), e);
			}
		}
	}

	@SuppressWarnings("SameParameterValue")
	private static <T> T requireInitialized(@Nullable T obj, String name) {
		return Objects.requireNonNull(obj, "INTERNAL ERROR: inconsistent state - " + name + " not initialized");
	}

	private static <T> T requireBody(@Nullable T body) {
		return Objects.requireNonNull(body, "Gateway returned a successful response with an empty body");
	}
}
