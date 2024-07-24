package io.bosonnetwork.higgs;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.bosonnetwork.BosonException;
import io.bosonnetwork.ConnectionStatusListener;
import io.bosonnetwork.CryptoContext;
import io.bosonnetwork.Id;
import io.bosonnetwork.Identity;
import io.bosonnetwork.LookupOption;
import io.bosonnetwork.Network;
import io.bosonnetwork.NodeInfo;
import io.bosonnetwork.NodeStatus;
import io.bosonnetwork.NodeStatusListener;
import io.bosonnetwork.PeerInfo;
import io.bosonnetwork.Result;
import io.bosonnetwork.Value;
import io.bosonnetwork.crypto.CryptoBox.Nonce;
import io.bosonnetwork.crypto.CryptoException;
import io.bosonnetwork.crypto.CryptoIdentity;
import io.bosonnetwork.utils.Base58;
import io.bosonnetwork.utils.Hex;
import io.bosonnetwork.utils.ThreadLocals;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;

public class Node extends AbstractVerticle implements io.bosonnetwork.Node {
	public static final int MAX_VALUE_AGE = 120 * 60 * 1000;
	public static final int RE_ANNOUNCE_INTERVAL = 5 * 60 * 1000;

	private static final String VERSION = "Higgs/1";

	private final Identity identity;
	private final String self;

	private final URL gateway;
	private String gatewayVersion;

	private LookupOption defaultLookupOption;

	private NodeStatus status;
	private List<NodeStatusListener> nodeStatusListeners;

	private Map<Id, PersistentValue> persistentValues;

	private ScheduledExecutorService scheduledExecutor;

	private AtomicReference<Vertx> instance;
	private WebClient webClient;

	private static final Logger log = LoggerFactory.getLogger(Node.class);

	Node(Identity identity, URL gateway) {
		this.identity = identity;
		this.self = identity.getId().toBase58String();
		this.gateway = gateway;
		this.defaultLookupOption = LookupOption.ARBITRARY;
		this.status = NodeStatus.Stopped;
		this.nodeStatusListeners = new ArrayList<>(4);
		this.persistentValues = new HashMap<>();
		this.instance = new AtomicReference<>(null);
	}

	public static io.bosonnetwork.Node create(URL gateway) {
		Objects.requireNonNull(gateway, "gateway");
		return new Node(new CryptoIdentity(), gateway);
	}

	public static io.bosonnetwork.Node create(byte[] privateKey, URL gateway) {
		Objects.requireNonNull(privateKey, "privateKey");
		Objects.requireNonNull(gateway, "gateway");
		return new Node(new CryptoIdentity(privateKey), gateway);
	}

	public static io.bosonnetwork.Node create(String gateway) {
		Objects.requireNonNull(gateway, "gateway");
		try {
			return new Node(new CryptoIdentity(), new URL(gateway));
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public static io.bosonnetwork.Node create(String privateKey, String gateway) {
		Objects.requireNonNull(privateKey, "privateKey");
		Objects.requireNonNull(gateway, "gateway");

		byte[] sk = privateKey.startsWith("0x") ? Hex.decode(privateKey) : Base58.decode(privateKey);
		try {
			return new Node(new CryptoIdentity(sk), new URL(gateway));
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException(e);
		}
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
	public boolean isLocalId(Id id) {
		Objects.requireNonNull(id, "id");
		return identity.getId().equals(id);
	}

	@Override
	public void setDefaultLookupOption(LookupOption option) {
		Objects.requireNonNull(option, "option");
		this.defaultLookupOption = option;
	}

	@Override
	public void addStatusListener(NodeStatusListener listener) {
		Objects.requireNonNull(listener, "listener");

		if (vertx == null) {
			synchronized(nodeStatusListeners) {
				if (!nodeStatusListeners.contains(listener))
					nodeStatusListeners.add(listener);
			}
		} else {
			vertx.runOnContext((v) -> {
				if (!nodeStatusListeners.contains(listener))
					nodeStatusListeners.add(listener);
			});
		}
	}

	@Override
	public void removeStatusListener(NodeStatusListener listener) {
		Objects.requireNonNull(listener, "listener");

		if (vertx == null) {
			synchronized(nodeStatusListeners) {
				nodeStatusListeners.remove(listener);
			}
		} else {
			vertx.runOnContext((v) -> {
				nodeStatusListeners.remove(listener);
			});
		}
	}

	@Override
	public void addConnectionStatusListener(ConnectionStatusListener listener) {
		// Prefer do nothing than throws exception.
		//
		//throw new UnsupportedOperationException("addConnectionStatusListener");
	}

	@Override
	public void removeConnectionStatusListener(ConnectionStatusListener listener) {
		// Prefer do nothing than throws exception.
		//
		//throw new UnsupportedOperationException("removeConnectionStatusListener");
	}

	@Override
	public void bootstrap(NodeInfo node) {
		// Prefer do nothing than throws exception.
		//
		// throw new UnsupportedOperationException("bootstrap");
	}

	@Override
	public void bootstrap(Collection<NodeInfo> bootstrapNodes) throws BosonException {
		// Prefer do nothing than throws exception.
		//
		// throw new UnsupportedOperationException("bootstrap");
	}

	private String generateAuthorization(Function<byte[], byte[]> checksum) throws CryptoException {
		Nonce nonce = Nonce.random();
		byte[] digest = checksum.apply(nonce.bytes());
		byte[] sig = identity.sign(digest);

		return "Bearer " + self + ":" + Base58.encode(nonce.bytes()) +
				":" + Base58.encode(sig);
	}

	private void persistentAnnounce() {
		log.info("Re-announce the persistent values...");

		long ts = System.currentTimeMillis() - MAX_VALUE_AGE + RE_ANNOUNCE_INTERVAL * 2;

		List<PersistentValue> values = persistentValues.values().stream()
				.filter((pv) -> pv.lastAnnounced() <= ts)
				.collect(Collectors.toList());

		vertx.executeBlocking(() -> {
			for (PersistentValue value : values) {
				vertx.runOnContext((v) -> {
					storeValue(value.value(), true).whenComplete((na, e) -> {
						if (e == null)
							log.debug("Re-announce the value {} success", value.value().getId());
						else
							log.error("Re-announce the value " + value.value().getId() + " failed", e);
					});
				});
			}

			return null;
		});
	}

	@Override
	public void start(Promise<Void> startPromise) {
		setStatus(NodeStatus.Stopped, NodeStatus.Starting);

        WebClientOptions options = new WebClientOptions()
				.setSsl(gateway.getProtocol().equals("https"))
				.setDefaultHost(gateway.getHost())
				.setDefaultPort(gateway.getPort() > 0 ? gateway.getPort() : gateway.getDefaultPort())
				.setProtocolVersion(HttpVersion.HTTP_1_1);

		webClient = WebClient.create(vertx, options);

		try {
			String auth = generateAuthorization((nonce) -> {
				MessageDigest shasum = ThreadLocals.sha256();
				shasum.reset();
				shasum.update(nonce);
				return shasum.digest();
			});

			webClient.get("/version")
				.putHeader("authorization", auth)
				.send()
				.andThen((ar) -> {
					if (ar.succeeded()) {
						if (ar.result().statusCode() == 200) {
							JsonObject resBody = ar.result().bodyAsJsonObject();
							gatewayVersion = resBody.getString("version");
							log.info("Gateway {} : version {}", gateway, gatewayVersion);
							setStatus(NodeStatus.Starting, NodeStatus.Running);

							vertx.setPeriodic(60000, RE_ANNOUNCE_INTERVAL, (tid) -> {
								persistentAnnounce();
							});

							startPromise.complete();
						} else {
							log.error("Gateway {} : status {}", gateway, ar.result().statusCode());
							webClient.close();
							setStatus(NodeStatus.Starting, NodeStatus.Stopped);
							startPromise.fail("Gateway error");
						}
					} else {
						log.error("Gateway {} : failed", gateway, ar.cause());
						webClient.close();
						setStatus(NodeStatus.Starting, NodeStatus.Stopped);
						startPromise.fail(ar.cause());
					}
				});
		} catch (Exception e) {
			if (webClient != null) {
				webClient.close();
				webClient = null;
			}

			setStatus(NodeStatus.Starting, NodeStatus.Stopped);
			startPromise.fail(e);
		}
	}

	@Override
	public void start() {
		if (instance.get() != null) {
			log.warn("Node already started");
		}

		VertxOptions options = new VertxOptions();
		options.setPreferNativeTransport(true);
		// options.setBlockedThreadCheckIntervalUnit(TimeUnit.SECONDS);
		// options.setBlockedThreadCheckInterval(300);
		Vertx v = Vertx.vertx(options);

		if (instance.compareAndSet(null, v)) {
			v.deployVerticle(this);
		}
	}

	@Override
	public void stop(Promise<Void> stopPromise) {
		setStatus(NodeStatus.Running, NodeStatus.Stopping);
		webClient.close();
		webClient = null;
		setStatus(NodeStatus.Stopping, NodeStatus.Stopped);
		stopPromise.complete();
	}

	@Override
	public synchronized void stop() {
		if (instance.compareAndSet(vertx, null)) {
			vertx.undeploy(this.deploymentID())
				.onComplete((ar) -> {
					vertx.close();
					log.info("Node stopped");
				});
		}
	}

	private void setStatus(NodeStatus expected, NodeStatus newStatus) {
		if (this.status.equals(expected)) {
			NodeStatus old = this.status;
			this.status = newStatus;
			if (!nodeStatusListeners.isEmpty()) {
				for (NodeStatusListener l : nodeStatusListeners) {
					l.statusChanged(newStatus, old);

					switch (newStatus) {
					case Starting:
						l.starting();
						break;

					case Running:
						l.started();
						break;

					case Stopping:
						l.stopping();
						break;

					case Stopped:
						l.stopped();
						break;

					default:
						break;
					}
				}
			}
		} else {
			log.warn("Set node status failed, expected is {}, actual is {}", expected, status);
		}
	}

	@Override
	public NodeStatus getStatus() {
		return status;
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
	public byte[] encrypt(Id recipient, byte[] data) {
		Objects.requireNonNull(recipient, "sender");
		Objects.requireNonNull(data, "data");
		return identity.encrypt(recipient, data);
	}

	@Override
	public byte[] decrypt(Id sender, byte[] data) throws BosonException {
		Objects.requireNonNull(sender, "sender");
		Objects.requireNonNull(data, "data");
		return identity.decrypt(sender, data);
	}

	@Override
	public CryptoContext createCryptoContext(Id id) {
		Objects.requireNonNull(id, "id");
		return identity.createCryptoContext(id);
	}

	@Override
	public CompletableFuture<Result<NodeInfo>> findNode(Id id, LookupOption option) {
		Objects.requireNonNull(id, "id");
		if (!isRunning())
			return CompletableFuture.failedFuture(new IllegalStateException("Node not running"));

		if (option == null)
			option = defaultLookupOption;

		CompletableFuture<Result<NodeInfo>> future = new CompletableFuture<>();

		try {
			String auth = generateAuthorization((nonce) -> {
				MessageDigest shasum = ThreadLocals.sha256();
				shasum.reset();
				shasum.update(nonce);
				shasum.update(id.bytes());
				return shasum.digest();
			});

			webClient.get("/nodes/" + id.toString())
				.addQueryParam("mode", option.name().toLowerCase())
				.putHeader("authorization", auth)
				.send()
				.onSuccess((res) -> {
					if (res.statusCode() == 200) {
						try {
							JsonObject body = res.bodyAsJsonObject();
							future.complete(nodeFromJson(body));
						} catch (Exception e) {
							log.error("INTERNAL ERROR", e);
							future.completeExceptionally(e);
						}
					} else if (res.statusCode() == 404) {
						future.complete(null);
					} else {
						log.error("Gateway error: status {}", res.statusCode());
						future.completeExceptionally(new IOException("HTTP status: " + res.statusCode()));
					}

				})
				.onFailure((e) -> {
					log.error("Gateway failed: {}", e.getMessage(), e);
					future.completeExceptionally(e);
				});
		} catch (Exception e) {
			future.completeExceptionally(e);
		}

		return future;
	}

	@Override
	public CompletableFuture<Value> findValue(Id id, LookupOption option) {
		Objects.requireNonNull(id, "id");
		if (!isRunning())
			return CompletableFuture.failedFuture(new IllegalStateException("Node not running"));

		if (option == null)
			option = defaultLookupOption;

		CompletableFuture<Value> future = new CompletableFuture<>();

		try {
			String auth = generateAuthorization((nonce) -> {
				MessageDigest shasum = ThreadLocals.sha256();
				shasum.reset();
				shasum.update(nonce);
				shasum.update(id.bytes());
				return shasum.digest();
			});

			webClient.get("/values/" + id.toString())
				.addQueryParam("mode", option.name().toLowerCase())
				.putHeader("authorization", auth)
				.send()
				.onSuccess((res) -> {
					if (res.statusCode() == 200) {
						try {
							JsonObject body = res.bodyAsJsonObject();
							future.complete(valueFromJson(body));
						} catch (Exception e) {
							log.error("INTERNAL ERROR", e);
							future.completeExceptionally(e);
						}
					} else if (res.statusCode() == 404) {
						future.complete(null);
					} else {
						log.error("Gateway error: status {}", res.statusCode());
						future.completeExceptionally(new IOException("HTTP status: " + res.statusCode()));
					}

				})
				.onFailure((e) -> {
					log.error("Gateway failed: {}", e.getMessage(), e);
					future.completeExceptionally(e);
				});
		} catch (Exception e) {
			future.completeExceptionally(e);
		}

		return future;
	}

	@Override
	public CompletableFuture<Void> storeValue(Value value, boolean persistent) {
		Objects.requireNonNull(value, "value");
		if (!isRunning())
			return CompletableFuture.failedFuture(new IllegalStateException("Node not running"));

		CompletableFuture<Void> future = new CompletableFuture<>();

		try {
			String auth = generateAuthorization((nonce) -> {
				MessageDigest shasum = ThreadLocals.sha256();
				shasum.reset();
				shasum.update(nonce);
				shasum.update(value.getId().bytes());
				if (value.isMutable())
					shasum.update(value.getSignature());
				return shasum.digest();
			});

			webClient.post("/values")
				.putHeader("authorization", auth)
				.sendJsonObject(valueToJson(value))
				.onSuccess((res) -> {
					if (res.statusCode() == 201) {
						if (persistent) {
							if (persistentValues.containsKey(value.getId())) {
								PersistentValue pv = persistentValues.get(value.getId());
								pv.updateLastAnnounced();
							} else {
								persistentValues.put(value.getId(), new PersistentValue(value));
							}
						}
						future.complete(null);
					} else {
						log.error("Gateway error: status {}", res.statusCode());
						future.completeExceptionally(new IOException("HTTP status: " + res.statusCode()));
					}

				})
				.onFailure((e) -> {
					log.error("Gateway failed: {}", e.getMessage(), e);
					future.completeExceptionally(e);
				});
		} catch (Exception e) {
			future.completeExceptionally(e);
		}

		return future;
	}

	@Override
	public CompletableFuture<List<PeerInfo>> findPeer(Id id, int expected, LookupOption option) {
		Objects.requireNonNull(id, "id");
		if (!isRunning())
			return CompletableFuture.failedFuture(new IllegalStateException("Node not running"));

		if (option == null)
			option = defaultLookupOption;

		CompletableFuture<List<PeerInfo>> future = new CompletableFuture<>();

		try {
			String auth = generateAuthorization((nonce) -> {
				MessageDigest shasum = ThreadLocals.sha256();
				shasum.reset();
				shasum.update(nonce);
				shasum.update(id.bytes());
				return shasum.digest();
			});

			HttpRequest<Buffer> request = webClient.get("/peers/" + id.toString());
			request.addQueryParam("mode", option.name().toLowerCase());
			if (expected > 0)
				request.addQueryParam("expected", Integer.toString(expected));

			request.putHeader("authorization", auth)
				.send()
				.onSuccess((res) -> {
					if (res.statusCode() == 200) {
						try {
							JsonArray body = res.bodyAsJsonArray();
							future.complete(peersFromJson(body));
						} catch (Exception e) {
							log.error("INTERNAL ERROR", e);
							future.completeExceptionally(e);
						}
					} else if (res.statusCode() == 404) {
						future.complete(null);
					} else {
						log.error("Gateway error: status {}", res.statusCode());
						future.completeExceptionally(new IOException("HTTP status: " + res.statusCode()));
					}

				})
				.onFailure((e) -> {
					log.error("Gateway failed: {}", e.getMessage(), e);
					future.completeExceptionally(e);
				});
		} catch (Exception e) {
			future.completeExceptionally(e);
		}

		return future;
	}

	@Override
	public CompletableFuture<Void> announcePeer(PeerInfo peer, boolean persistent) {
		return CompletableFuture.failedFuture(new UnsupportedOperationException("announcePeer"));
	}

	@Override
	public CompletableFuture<Value> getValue(Id valueId) {
		Objects.requireNonNull(valueId, "valueId");

		if (vertx == null) {
			synchronized(persistentValues) {
				PersistentValue value = persistentValues.get(valueId);
				return CompletableFuture.completedFuture(value == null ? null : value.value());
			}
		} else {
			CompletableFuture<Value> future = new CompletableFuture<>();
			vertx.runOnContext((v) -> {
				PersistentValue value = persistentValues.get(valueId);
				 future.complete(value == null ? null : value.value());
			});

			return future;
		}
	}

	@Override
	public CompletableFuture<Boolean> removeValue(Id valueId) {
		Objects.requireNonNull(valueId, "valueId");

		if (vertx == null) {
			synchronized(persistentValues) {
				PersistentValue value = persistentValues.remove(valueId);
				return CompletableFuture.completedFuture(value != null);
			}
		} else {
			CompletableFuture<Boolean> future = new CompletableFuture<>();
			vertx.runOnContext((v) -> {
				PersistentValue value = persistentValues.remove(valueId);
				 future.complete(value != null);
			});

			return future;
		}
	}

	@Override
	public CompletableFuture<PeerInfo> getPeer(Id peerId) {
		return CompletableFuture.failedFuture(new UnsupportedOperationException("getPeer"));
	}

	@Override
	public CompletableFuture<Boolean> removePeer(Id peerId) {
		return CompletableFuture.failedFuture(new UnsupportedOperationException("removePeer"));
	}

	@Override
	public String getVersion() {
		return VERSION + ":" + (gatewayVersion != null ? gatewayVersion : "N/A");
	}

	private Result<NodeInfo> nodeFromJson(JsonObject json) {
		NodeInfo n4 = null;
		if (json.containsKey(Network.IPv4.name())) {
			JsonObject ni = json.getJsonObject(Network.IPv4.name());
			n4 = new NodeInfo(Id.of(ni.getString("id")),
					ni.getString("ip"), ni.getInteger("port"));

			if (ni.containsKey("version"))
				n4.setVersion(ni.getInteger("version"));
		}

		NodeInfo n6 = null;
		if (json.containsKey(Network.IPv6.name())) {
			JsonObject ni = json.getJsonObject(Network.IPv6.name());
			n6 = new NodeInfo(Id.of(ni.getString("id")),
					ni.getString("ip"), ni.getInteger("port"));

			if (ni.containsKey("version"))
				n6.setVersion(ni.getInteger("version"));
		}

		return new Result<>(n4, n6);
	}

	private List<PeerInfo> peersFromJson(JsonArray json) {
		return json.stream().map((o) -> {
			JsonObject j = (JsonObject)o;

			return PeerInfo.of(Id.of(j.getString("id")), Id.of(j.getString("nodeId")),
					Id.of(j.getString("origin")), j.getInteger("port"), j.getString("alt"),
					Hex.decode(j.getString("signature")));
		}).collect(Collectors.toList());
	}

	private JsonObject valueToJson(Value value) {
		JsonObject object = new JsonObject();

		if (value.isMutable()) {
			object.put("pk", value.getPublicKey().toString());
			if (value.getRecipient() != null)
				object.put("rec", value.getRecipient().toString());
			object.put("nonce", Hex.encode(value.getNonce()));
			object.put("seq", value.getSequenceNumber());
			object.put("sig", Hex.encode(value.getSignature()));
		}
		object.put("data", value.getData());

		return object;
	}

	private Value valueFromJson(JsonObject object) {
		String v = object.getString("pk");
		Id pk = v != null ? Id.of(v) : null;

		Id rec = null;
		byte[] nonce = null;
		int seq = 0;
		byte[] sig = null;

		if (pk != null) {
			v = object.getString("rec");
			if (v != null)
				rec = Id.of(v);

			v = object.getString("nonce");
			if (v != null)
				nonce = Hex.decode(v);

			seq = object.getInteger("seq", 0);

			v = object.getString("sig");
			if (v != null)
				sig = Hex.decode(v);
		}

		byte[] data = object.getBinary("data");

		Value value = Value.of(pk, rec, nonce, seq, sig, data);
		if (!value.isValid())
			throw new IllegalArgumentException("Invalid json for value");

		return value;
	}

	@Override
	public ScheduledExecutorService getScheduler() {
		if (scheduledExecutor == null)
			scheduledExecutor = new VertxScheduledExecutorService(vertx);

		return scheduledExecutor;
	}
}
