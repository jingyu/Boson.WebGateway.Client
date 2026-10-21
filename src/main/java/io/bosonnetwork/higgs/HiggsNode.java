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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.fasterxml.jackson.core.type.TypeReference;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.TrustOptions;
import io.vertx.core.http.HttpHeaders;
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
import io.bosonnetwork.Subscription;
import io.bosonnetwork.SubscriptionListener;
import io.bosonnetwork.Value;
import io.bosonnetwork.crypto.CryptoException;
import io.bosonnetwork.crypto.CryptoIdentity;
import io.bosonnetwork.crypto.HybridTrustManager;
import io.bosonnetwork.crypto.Signature;
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
import io.bosonnetwork.web.HttpDate;
import io.bosonnetwork.web.client.AccessTokenSource;
import io.bosonnetwork.web.client.SelfIssuedAccessTokens;

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
 * {@link CompletableFuture}s complete on the caller's Vert.x context. A Vert.x caller can turn one
 * back into a {@link io.vertx.core.Future} with {@code Future.fromCompletionStage}.
 *
 * <h2>Subscriptions</h2>
 * {@link #subscribeValue} and {@link #subscribePeer} are held by the gateway for this node's device and
 * pushed over one Server-Sent Events stream, which the device's first subscription opens and which is
 * deleted a grace period ({@link Builder#subscriptionGracePeriod}) after its last is cancelled. The stream
 * belongs to the device: two clients must not use the same device key, and the gateway ends the older
 * one's subscriptions when they do. Listeners are called on this node's own context. On Android a
 * connection does not survive Doze, so subscriptions are for the foreground: the stream reconnects, and
 * catches up, when the app is back.
 *
 * <h2>Unsupported operations</h2>
 * Some {@link Node} methods are not meaningful for a gateway client: {@link #getNodeInfo()} throws
 * {@link UnsupportedOperationException}, while {@link #bootstrap(NodeInfo) bootstrap},
 * {@link #addConnectionStatusListener} and {@link #removeConnectionStatusListener} are no-ops.
 *
 * <p>Instances are obtained through {@link #builder()}.
 */
public class HiggsNode implements Node {

	private static final String VERSION = "Higgs/1";

	// current support API version prefix
	private static final String API_VERSION_PREFIX = "/v1";

	private final Vertx vertx;

	private final Id userId;
	private final Identity deviceIdentity;

	private @Nullable Id gatewayNodeId;
	private final Id gatewayPeerId;
	private final URL gatewayUrl;
	private final String basePath;

	private LookupOption defaultLookupOption;

	private volatile @Nullable String gatewayVersion;
	private @Nullable HttpClient httpClient;
	private @Nullable WebClient webClient;
	private volatile @Nullable GatewaySubscriptions subscriptions;
	private final long subscriptionGracePeriod;

	private final AccessTokenSource tokens;

	private final AtomicBoolean running;

	private static final Logger log = LoggerFactory.getLogger(HiggsNode.class);

	private HiggsNode(Builder builder) {
		this.vertx = Objects.requireNonNull(builder.vertx, "Vert.x instance must be set");

		this.userId = Objects.requireNonNull(builder.userId, "Either userId or userKey must be set");

		Objects.requireNonNull(builder.deviceKey, "deviceKey must be set");
		this.deviceIdentity = new CryptoIdentity(builder.deviceKey);

		this.gatewayPeerId = Objects.requireNonNull(builder.gatewayPeerId, "gatewayPeerId must be set");
		this.gatewayUrl = Objects.requireNonNull(builder.gatewayUrl, "gatewayUrl must be set");

		// The device signs its own short-lived tokens on behalf of the user, bound to this gateway, and
		// dates them by the gateway's clock if a refusal shows ours to be off.
		this.tokens = SelfIssuedAccessTokens.builder(deviceIdentity)
				.subject(userId)
				.clientId(deviceIdentity.getId())
				.scope(AccessScope.CLIENT)
				.audience(gatewayPeerId)
				.logger(log)
				.build();

		String path = gatewayUrl.getPath().replaceAll("/+$", "");
		this.basePath = path + API_VERSION_PREFIX;

		this.defaultLookupOption = LookupOption.ARBITRARY;
		this.subscriptionGracePeriod = builder.subscriptionGracePeriod;
		this.running = new AtomicBoolean(false);
	}

	/**
	 * Returns a one-line description of the gateway this node is bound to, for logs and
	 * diagnostics: the gateway URL and peer id, plus the node id and the gateway version learned
	 * from {@code /info} at {@link #start()} once available (the version reads {@code N/A} before
	 * a successful start).
	 *
	 * @return the gateway description
	 */
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
	public CompletableFuture<Void> start() {
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
			// HTTP/2 when the gateway offers it, negotiated by ALPN, HTTP/1.1 otherwise: one connection
			// then carries the subscription stream and the requests beside it. Cleartext stays on
			// HTTP/1.1, which needs no negotiation.
			options.setEnabledSecureTransportProtocols(Set.of("TLSv1.2", "TLSv1.3"))
					.setTrustOptions(TrustOptions.wrap(new HybridTrustManager(gatewayPeerId.toString(), gatewayPeerId.bytesUnsafe())))
					.setUseAlpn(true)
					.setProtocolVersion(HttpVersion.HTTP_2);
		}

		// One client for the requests and the subscription stream, which needs the core client to read
		// its body as it arrives.
		httpClient = vertx.createHttpClient(options);
		webClient = WebClient.wrap(httpClient, options);
		subscriptions = new GatewaySubscriptions(vertx.getOrCreateContext(), new SubscriptionTransport(),
				subscriptionGracePeriod);

		Future<Void> future = fetchGatewayInfo().compose(info -> {
			Id peerId = Id.of(info.getString("peerId"));
			if (!peerId.equals(gatewayPeerId))
				return Future.failedFuture(new HiggsException(HiggsException.NO_HTTP_STATUS, "Gateway peer ID mismatch"));

			this.gatewayNodeId = Id.of(info.getString("nodeId"));

			this.gatewayVersion = info.getString("version");
			return Future.succeededFuture();
		}).onFailure(e -> {
			log.error("Failed to get gateway info", e);
			closeClients();
			running.set(false);
		}).mapEmpty();

		return ContextualFuture.of(future);
	}

	@Override
	public CompletableFuture<Void> stop() {
		if (!running.compareAndSet(true, false))
			return ContextualFuture.failedFuture(new IllegalStateException("Not started"));

		// The subscription stream is deleted before the client closes; a gateway that cannot be told
		// expires it on its own.
		GatewaySubscriptions subs = subscriptions;
		subscriptions = null;
		Future<Void> stopped = subs != null ? subs.stop() : Future.succeededFuture();
		return ContextualFuture.of(stopped.eventually(() -> {
			closeClients();
			return Future.succeededFuture();
		}));
	}

	private void closeClients() {
		subscriptions = null;
		if (webClient != null) {
			webClient.close();
			webClient = null;
		}
		if (httpClient != null) {
			httpClient.close();
			httpClient = null;
		}
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
	public CompletableFuture<Void> bootstrap(NodeInfo node) {
		return ContextualFuture.succeededFuture();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @implNote No-op in the gateway client (see {@link #bootstrap(NodeInfo)}).
	 */
	@Override
	public CompletableFuture<Void> bootstrap(Collection<NodeInfo> bootstrapNodes) {
		return ContextualFuture.succeededFuture();
	}

	private String uri(Object... suffixes) {
		StringBuilder sb = new StringBuilder(basePath);
		for (Object suffix : suffixes)
			sb.append('/').append(suffix);
		return sb.toString();
	}

	@Override
	public CompletableFuture<Optional<NodeInfo>> findNode(Id id, @Nullable LookupOption option) {
		Objects.requireNonNull(id, "id");
		runningCheck();

		final LookupOption lookupOption = option != null ? option : defaultLookupOption;
		WebClient webClient = requireInitialized(this.webClient, "webClient");

		Future<Optional<NodeInfo>> future = sendAuthenticated(webClient.get(uri("nodes", id))
				.addQueryParam("mode", lookupOption.name().toLowerCase()),
				HttpRequest::send)
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
	public CompletableFuture<Optional<Value>> findValue(Id id, int expectedSequenceNumber, @Nullable LookupOption option) {
		Objects.requireNonNull(id, "id");
		if (expectedSequenceNumber < -1)
			throw new IllegalArgumentException("expectedSequenceNumber must be >= -1");
		runningCheck();

		final LookupOption lookupOption = option != null ? option : defaultLookupOption;
		WebClient webClient = requireInitialized(this.webClient, "webClient");

		HttpRequest<Buffer> request = webClient.get(uri("values", id));
		request.addQueryParam("mode", lookupOption.name().toLowerCase());
		if (expectedSequenceNumber >= 0)
			request.addQueryParam("seq", String.valueOf(expectedSequenceNumber));

		Future<Optional<Value>> future = sendAuthenticated(request, HttpRequest::send)
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
	public CompletableFuture<AnnounceResult> storeValue(Value value, int expectedSequenceNumber, boolean persistent) {
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
		Future<AnnounceResult> future = sendAuthenticated(webClient.post(uri("values")),
				r -> r.sendJsonObject(body))
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
	public CompletableFuture<List<PeerInfo>> findPeer(Id id, int expectedSequenceNumber, int expectedCount, @Nullable LookupOption option) {
		Objects.requireNonNull(id, "id");
		if (expectedSequenceNumber < -1)
			throw new IllegalArgumentException("expectedSequenceNumber must be >= -1");
		if (expectedCount < 0)
			throw new IllegalArgumentException("expectedCount must be >= 0");
		runningCheck();

		final LookupOption lookupOption = option != null ? option : defaultLookupOption;
		WebClient webClient = requireInitialized(this.webClient, "webClient");

		HttpRequest<Buffer> request = webClient.get(uri("peers", id));
		request.addQueryParam("mode", lookupOption.name().toLowerCase());
		if (expectedSequenceNumber >= 0)
			request.addQueryParam("seq", Integer.toString(expectedSequenceNumber));
		request.addQueryParam("count", Integer.toString(expectedCount));

		Future<List<PeerInfo>> future = sendAuthenticated(request, HttpRequest::send)
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
	public CompletableFuture<AnnounceResult> announcePeer(PeerInfo peer, int expectedSequenceNumber, boolean persistent) {
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
		Future<AnnounceResult> future = sendAuthenticated(webClient.post(uri("peers")),
				r -> r.sendJsonObject(body))
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
	public CompletableFuture<Optional<Value>> getValue(Id valueId) {
		Objects.requireNonNull(valueId, "valueId");
		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		Future<Optional<Value>> future = sendAuthenticated(webClient.get(uri("user/values", valueId)),
				HttpRequest::send)
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

	/**
	 * Lists the values persisted by the acting user, page by page.
	 * <p>
	 * Where {@link #getValue(Id)} reads one value by id, this is the account view: every persistent
	 * value the gateway holds for the user. A user with no stored values completes with an empty
	 * page rather than a failure.
	 *
	 * @param page     the 1-based page index (must be {@code >= 1})
	 * @param pageSize the number of items per page (must be {@code >= 1}; {@link Long#MAX_VALUE}
	 *                 with {@code page} {@code 1} asks the gateway for everything unpaged)
	 * @return a future completing with the requested page of values
	 * @throws IllegalArgumentException if {@code page} or {@code pageSize} is less than 1
	 * @throws IllegalStateException    if this node is not running
	 */
	public CompletableFuture<PaginatedResult<Value>> getAllValues(long page, long pageSize) {
		if (page <= 0)
			throw new IllegalArgumentException("page must be >= 1");
		if (pageSize <= 0)
			throw new IllegalArgumentException("pageSize must be >= 1");

		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		HttpRequest<Buffer> request = webClient.get(uri("user/values"));
		if (page > 1 || pageSize != Long.MAX_VALUE) {
			request.addQueryParam("page", Long.toString(page));
			request.addQueryParam("pageSize", Long.toString(pageSize));
		}

		Future<PaginatedResult<Value>> future = sendAuthenticated(request,
				HttpRequest::send)
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
	public CompletableFuture<Boolean> removeValue(Id valueId) {
		Objects.requireNonNull(valueId, "valueId");
		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		Future<Boolean> future = sendAuthenticated(webClient.delete(uri("user/values", valueId)),
				HttpRequest::send)
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
	public CompletableFuture<List<PeerInfo>> getPeers(Id peerId) {
		Objects.requireNonNull(peerId, "peerId");
		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		Future<List<PeerInfo>> future = sendAuthenticated(webClient.get(uri("user/peers", peerId)),
				HttpRequest::send)
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

	/**
	 * Lists the peer infos announced by the acting user, page by page.
	 * <p>
	 * Where {@link #getPeers(Id)} reads the replicas recorded under one peer id, this is the
	 * account view: every persistent peer info the gateway holds for the user. A user with nothing
	 * announced completes with an empty page rather than a failure.
	 *
	 * @param page     the 1-based page index (must be {@code >= 1})
	 * @param pageSize the number of items per page (must be {@code >= 1}; {@link Long#MAX_VALUE}
	 *                 with {@code page} {@code 1} asks the gateway for everything unpaged)
	 * @return a future completing with the requested page of peer infos
	 * @throws IllegalArgumentException if {@code page} or {@code pageSize} is less than 1
	 * @throws IllegalStateException    if this node is not running
	 */
	public CompletableFuture<PaginatedResult<PeerInfo>> getAllPeers(long page, long pageSize) {
		if (page <= 0)
			throw new IllegalArgumentException("page must be >= 1");
		if (pageSize <= 0)
			throw new IllegalArgumentException("pageSize must be >= 1");

		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		HttpRequest<Buffer> request = webClient.get(uri("user/peers"));
		if (page > 1 || pageSize != Long.MAX_VALUE) {
			request.addQueryParam("page", Long.toString(page));
			request.addQueryParam("pageSize", Long.toString(pageSize));
		}

		Future<PaginatedResult<PeerInfo>> future = sendAuthenticated(request,
				HttpRequest::send)
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
	public CompletableFuture<Boolean> removePeers(Id peerId) {
		Objects.requireNonNull(peerId, "peerId");
		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		Future<Boolean> future = sendAuthenticated(webClient.delete(uri("user/peers", peerId)),
				HttpRequest::send)
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
	public CompletableFuture<Optional<PeerInfo>> getPeer(Id peerId, long fingerprint) {
		Objects.requireNonNull(peerId, "peerId");
		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		Future<Optional<PeerInfo>> future = sendAuthenticated(webClient.get(uri("user/peers", peerId, fingerprint)),
				HttpRequest::send)
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
	public CompletableFuture<Boolean> removePeer(Id peerId, long fingerprint) {
		Objects.requireNonNull(peerId, "peerId");
		runningCheck();

		WebClient webClient = requireInitialized(this.webClient, "webClient");
		Future<Boolean> future = sendAuthenticated(webClient.delete(uri("user/peers", peerId, fingerprint)),
				HttpRequest::send)
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

	/**
	 * {@inheritDoc}
	 *
	 * @implNote Held by the gateway for this node's device, and pushed over its event stream; see the
	 *           class documentation. The future fails if the gateway refuses the target - this user's
	 *           subscriptions are at their limit, or the gateway is full - or another client uses this
	 *           device.
	 */
	@Override
	public CompletableFuture<Subscription> subscribeValue(Id id, int knownSequenceNumber,
			SubscriptionListener<Value> listener) {
		Objects.requireNonNull(id, "id");
		Objects.requireNonNull(listener, "listener");
		if (knownSequenceNumber < -1)
			throw new IllegalArgumentException("knownSequenceNumber must be >= -1");
		runningCheck();

		GatewaySubscriptions subs = requireInitialized(subscriptions, "subscriptions");
		return ContextualFuture.of(subs.subscribe(GatewaySubscriptions.Kind.VALUE, id, knownSequenceNumber, listener));
	}

	/**
	 * {@inheritDoc}
	 *
	 * @implNote As {@link #subscribeValue(Id, int, SubscriptionListener)}.
	 */
	@Override
	public CompletableFuture<Subscription> subscribePeer(Id id, SubscriptionListener<PeerInfo> listener) {
		Objects.requireNonNull(id, "id");
		Objects.requireNonNull(listener, "listener");
		runningCheck();

		GatewaySubscriptions subs = requireInitialized(subscriptions, "subscriptions");
		return ContextualFuture.of(subs.subscribe(GatewaySubscriptions.Kind.PEER, id, -1, listener));
	}

	// The gateway calls of the subscriptions, authenticated as every other request.
	private final class SubscriptionTransport implements GatewaySubscriptions.Transport {
		@Override
		public Future<HttpResponse<Buffer>> subscribe(JsonObject body) {
			WebClient client = requireInitialized(webClient, "webClient");
			return sendAuthenticated(client.post(uri("subscriptions")), r -> r.sendJsonObject(body));
		}

		@Override
		public Future<HttpResponse<Buffer>> unsubscribe(String kind, Id id) {
			WebClient client = requireInitialized(webClient, "webClient");
			return sendAuthenticated(client.delete(uri("subscriptions", kind, id)), HttpRequest::send);
		}

		@Override
		public Future<HttpResponse<Buffer>> deleteStream() {
			WebClient client = requireInitialized(webClient, "webClient");
			return sendAuthenticated(client.delete(uri("subscriptions")), HttpRequest::send);
		}

		@Override
		public Future<HttpClientResponse> openStream(@Nullable String lastEventId) {
			return ContextualFuture.callOnVertxThread(vertx, () -> tokens.token().compose(token ->
					openStream(token, lastEventId).compose(res -> {
						// A refusal that blames our clock is retried once with a token it dates right.
						if (res.statusCode() != 401 ||
								!tokens.rejected(token, HttpDate.parse(res.getHeader(HttpHeaders.DATE))))
							return Future.succeededFuture(res);

						res.body();
						return tokens.token().compose(fresh -> openStream(fresh, lastEventId));
					})));
		}

		private Future<HttpClientResponse> openStream(String token, @Nullable String lastEventId) {
			HttpClient client = requireInitialized(httpClient, "httpClient");
			RequestOptions request = new RequestOptions()
					.setMethod(HttpMethod.GET)
					.setURI(uri("subscriptions", "events"))
					.putHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token)
					.putHeader(HttpHeaders.ACCEPT, "text/event-stream");
			if (lastEventId != null)
				request.putHeader("Last-Event-ID", lastEventId);
			// Paused before any of the body arrives, until the reader has its handlers in place.
			return client.request(request).compose(req -> req.send()).map(res -> {
				if (res.statusCode() == 200)
					res.pause();
				return res;
			});
		}
	}

	private Future<JsonObject> fetchGatewayInfo() {
		WebClient webClient = requireInitialized(this.webClient, "webClient");
		return sendAuthenticated(webClient.get(uri("info")),
				HttpRequest::send)
				.compose(res -> {
					if (res.statusCode() == 200)
						return Future.succeededFuture(res.bodyAsJsonObject());
					else
						return Future.failedFuture(wrapErrorResponseToException(res));
				});
	}

	// Sends an authenticated request, and repeats it once if the gateway refuses the token and the
	// token source says another would do better - which is how a client clock too far from the
	// gateway's own rights itself.
	private Future<HttpResponse<Buffer>> sendAuthenticated(HttpRequest<Buffer> request,
			Function<HttpRequest<Buffer>, Future<HttpResponse<Buffer>>> send) {
		// Issued from a Vert.x thread, where the host lookup may block: see
		// ContextualFuture.callOnVertxThread.
		return ContextualFuture.callOnVertxThread(vertx, () -> tokens.token()
				.compose(token -> send.apply(request.bearerTokenAuthentication(token))
				.compose(res -> {
					// The date of the refusal is how the token source learns that our clock, not our
					// key, is what the gateway objected to.
					if (res.statusCode() != 401 ||
							!tokens.rejected(token, HttpDate.parse(res.getHeader(HttpHeaders.DATE))))
						return Future.succeededFuture(res);

					return tokens.token().compose(fresh -> send.apply(request.bearerTokenAuthentication(fresh)));
				})));
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
		private long subscriptionGracePeriod = 10_000;

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
		/**
		 * Sets how long the subscription stream is kept after its last subscription is cancelled, so
		 * that an app cancelling and subscribing again keeps it. Default 10 seconds.
		 *
		 * @param millis the grace period in milliseconds
		 * @return this builder
		 * @throws IllegalArgumentException if negative
		 */
		public Builder subscriptionGracePeriod(long millis) {
			if (millis < 0)
				throw new IllegalArgumentException("Invalid subscriptionGracePeriod");
			this.subscriptionGracePeriod = millis;
			return this;
		}

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
