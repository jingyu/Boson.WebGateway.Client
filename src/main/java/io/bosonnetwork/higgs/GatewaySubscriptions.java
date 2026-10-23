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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.bosonnetwork.Id;
import io.bosonnetwork.PeerInfo;
import io.bosonnetwork.Subscription;
import io.bosonnetwork.SubscriptionListener;
import io.bosonnetwork.Value;
import io.bosonnetwork.higgs.exceptions.HiggsException;
import io.bosonnetwork.utils.MonotonicClock;
import io.bosonnetwork.vertx.ContextualFuture;

/**
 * A {@link HiggsNode}'s DHT subscriptions, held by its gateway and read from the gateway's event stream.
 * <p>
 * <b>One stream per client.</b> The gateway keeps one stream per device; this node's device subscribes
 * its targets to it ({@code POST /v1/subscriptions}) and reads it ({@code GET /v1/subscriptions/events}).
 * Several local subscriptions of one target share one target on the gateway, each with its own view of
 * what it has been delivered. The stream is deleted a grace period after the last local subscription is
 * cancelled, so a subscription cancelled and taken again keeps it.
 * </p>
 * <p>
 * <b>A standing request.</b> A dropped connection does not end a subscription: the reader reconnects with
 * {@code Last-Event-ID} and is replayed what it missed, reporting {@code onStatusChange(0)} while away. A
 * stream the gateway no longer has ({@code 404}) is subscribed again, each target with what it last
 * delivered. Only {@link Subscription#cancel()} and {@link HiggsNode#stop()} end subscriptions - and the
 * gateway saying another client uses this device ({@code replaced}), which they cannot recover from.
 * </p>
 * <p>
 * Records are validated here as well; the gateway is not trusted to have done it. Every method runs on
 * one Vert.x context, where listeners are called.
 * </p>
 */
final class GatewaySubscriptions {
	/** The calls this class needs from its node: authenticated, and answered on any context. */
	interface Transport {
		Future<HttpResponse<Buffer>> subscribe(JsonObject body);

		Future<HttpResponse<Buffer>> unsubscribe(String kind, Id id);

		Future<HttpResponse<Buffer>> deleteStream();

		/**
		 * Opens the event stream; the response, whatever its status, with its body not yet read. A 200 is
		 * answered paused: its first events would otherwise be gone before a handler is set.
		 */
		Future<HttpClientResponse> openStream(@Nullable String lastEventId);
	}

	enum Kind {
		VALUE, PEER;

		String value() {
			return name().toLowerCase(Locale.ROOT);
		}
	}

	private record Key(Kind kind, Id id) {
	}

	// How many targets one subscribe request carries; the gateway takes up to 64.
	private static final int TARGETS_PER_REQUEST = 32;
	private static final long MAX_BACKOFF_MILLIS = 60_000;
	// A connection silent this long is taken for dead: the gateway sends a heartbeat every 25 seconds.
	private static final long SILENCE_MILLIS = 90_000;
	// How long a target the gateway refused waits before it is asked again.
	private static final long REFUSED_RETRY_MILLIS = 30_000;

	private final Context context;
	private final Transport transport;
	private final long gracePeriod;

	private final Map<Key, Entry> entries = new LinkedHashMap<>();

	private @Nullable HttpClientResponse reader;
	private boolean connecting;
	private long reconnectTimer;
	private long watchdogTimer;
	private long graceTimer;
	private long lastBytesAt;
	private @Nullable String lastEventId;
	private long retryMillis = 3000;
	private int attempts;
	private boolean stopped;
	private boolean replaced;

	private static final Logger log = LoggerFactory.getLogger(GatewaySubscriptions.class);

	GatewaySubscriptions(Context context, Transport transport, long gracePeriod) {
		this.context = context;
		this.transport = transport;
		this.gracePeriod = gracePeriod;
	}

	// One target on the gateway, and the local subscriptions sharing it.
	private final class Entry {
		final Key key;
		final List<Sub> subs = new ArrayList<>();
		int subscribedNodes = -1;
		@Nullable Value latestValue;
		final Map<Long, PeerInfo> latestPeers = new LinkedHashMap<>();
		long retryTimer;

		Entry(Key key) {
			this.key = key;
		}

		// What to resume from when the target is subscribed again.
		int knownSequenceNumber() {
			if (key.kind != Kind.VALUE)
				return -1;
			if (latestValue != null)
				return latestValue.getSequenceNumber();
			int known = Integer.MAX_VALUE;
			for (Sub sub : subs)
				known = Math.min(known, sub.lastSequenceNumber);
			return subs.isEmpty() ? -1 : known;
		}
	}

	private final class Sub implements Subscription {
		final Entry entry;
		final SubscriptionListener<Object> listener;
		final Promise<Subscription> ready = Promise.promise();
		int lastSequenceNumber;
		final Map<Long, Integer> peers = new HashMap<>();
		boolean cancelled;
		volatile int subscribedNodes;

		@SuppressWarnings("unchecked")
		Sub(Entry entry, SubscriptionListener<?> listener, int knownSequenceNumber) {
			this.entry = entry;
			this.listener = (SubscriptionListener<Object>) listener;
			this.lastSequenceNumber = knownSequenceNumber;
		}

		@Override
		public Id getTarget() {
			return entry.key.id;
		}

		@Override
		public int getSubscribedNodes() {
			return subscribedNodes;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public CompletableFuture<Void> cancel() {
			return ContextualFuture.of(ContextualFuture.callOnContext(context, () -> {
				release(this);
				return Future.<Void>succeededFuture();
			}));
		}

		void deliver(Object record) {
			if (cancelled)
				return;
			if (record instanceof Value value) {
				if (value.getSequenceNumber() <= lastSequenceNumber)
					return;
				lastSequenceNumber = value.getSequenceNumber();
			} else if (record instanceof PeerInfo peer) {
				Integer known = peers.get(peer.getFingerprint());
				if (known != null && peer.getSequenceNumber() <= known)
					return;
				peers.put(peer.getFingerprint(), peer.getSequenceNumber());
			}
			try {
				listener.onUpdate(record);
			} catch (Exception e) {
				log.error("Subscription listener of {} failed", entry.key.id, e);
			}
		}

		void status(int nodes) {
			if (cancelled)
				return;
			subscribedNodes = nodes;
			ready.tryComplete(this);
			try {
				listener.onStatusChange(nodes);
			} catch (Exception e) {
				log.error("Subscription listener of {} failed", entry.key.id, e);
			}
		}
	}

	/**
	 * Subscribes to a target.
	 *
	 * @return the subscription, once the gateway has said how many storing nodes watch the target
	 */
	Future<Subscription> subscribe(Kind kind, Id id, int knownSequenceNumber, SubscriptionListener<?> listener) {
		return ContextualFuture.callOnContext(context, () -> {
			if (stopped)
				return Future.failedFuture(new IllegalStateException("Node not running"));
			if (replaced)
				return Future.failedFuture(replacedError());

			cancelGrace();
			Key key = new Key(kind, id);
			Entry entry = entries.get(key);
			if (entry != null) {
				// Shared with an earlier local subscription: bring this one up to date from what the
				// stream has delivered.
				Sub sub = new Sub(entry, listener, knownSequenceNumber);
				entry.subs.add(sub);
				if (entry.latestValue != null)
					sub.deliver(entry.latestValue);
				for (PeerInfo peer : entry.latestPeers.values())
					sub.deliver(peer);
				if (entry.subscribedNodes >= 0)
					sub.status(reader != null ? entry.subscribedNodes : 0);
				ensureReader();
				return sub.ready.future();
			}

			Entry fresh = new Entry(key);
			entries.put(key, fresh);
			Sub sub = new Sub(fresh, listener, knownSequenceNumber);
			fresh.subs.add(sub);
			return post(List.of(fresh)).compose(v -> {
				ensureReader();
				return sub.ready.future();
			});
		});
	}

	// Subscribes targets on the gateway. Every target's outcome is dealt with here (see refused), so the
	// future only says the requests are done.
	private Future<Void> post(List<Entry> targets) {
		List<Future<Void>> requests = new ArrayList<>();
		for (int i = 0; i < targets.size(); i += TARGETS_PER_REQUEST) {
			List<Entry> chunk = targets.subList(i, Math.min(targets.size(), i + TARGETS_PER_REQUEST));
			JsonArray body = new JsonArray();
			for (Entry entry : chunk) {
				JsonObject target = new JsonObject().put("kind", entry.key.kind.value()).put("id", entry.key.id.toString());
				if (entry.key.kind == Kind.VALUE)
					target.put("knownSequenceNumber", entry.knownSequenceNumber());
				body.add(target);
			}

			requests.add(onContext(transport.subscribe(new JsonObject().put("targets", body))).<Void>transform(ar -> {
				if (ar.failed()) {
					HiggsException error = ar.cause() instanceof HiggsException he ? he :
							new HiggsException("Gateway request failed", ar.cause());
					for (Entry entry : chunk)
						refused(entry, error);
				} else if (ar.result().statusCode() != 200) {
					HiggsException error = HiggsException.fromResponse(ar.result());
					for (Entry entry : chunk)
						refused(entry, error);
				} else {
					JsonObject answer = ar.result().bodyAsJsonObject();
					JsonArray results = answer != null ? answer.getJsonArray("targets", new JsonArray()) : new JsonArray();
					for (int j = 0; j < chunk.size(); j++) {
						JsonObject result = j < results.size() ? results.getJsonObject(j) : new JsonObject();
						if (!result.getBoolean("accepted", false))
							refused(chunk.get(j), new HiggsException(403,
									"Subscription refused: " + result.getString("reason", "refused")));
					}
				}
				return Future.succeededFuture();
			}));
		}
		return Future.join(requests).mapEmpty();
	}

	// A target the gateway did not take, or no longer watches. Subscriptions still waiting for their first
	// status fail with the reason; standing ones report no node watching, and the target is asked again
	// later. A target with nothing left standing is dropped.
	private void refused(Entry entry, Throwable error) {
		if (entries.get(entry.key) != entry)
			return;

		for (Sub sub : entry.subs) {
			if (!sub.ready.future().isComplete())
				sub.ready.tryFail(error);
		}

		// No Stream.toList(): Android 13, which this client runs on, does not have it.
		List<Sub> standing = new ArrayList<>();
		for (Sub sub : entry.subs) {
			if (sub.ready.future().succeeded())
				standing.add(sub);
		}
		if (standing.isEmpty()) {
			entry.subs.clear();
			drop(entry);
			return;
		}

		log.debug("Subscription of {} refused by the gateway: {}; asking again later", entry.key.id, error.getMessage());
		for (Sub sub : standing)
			sub.status(0);
		retryLater(entry);
	}

	private void drop(Entry entry) {
		entries.remove(entry.key);
		if (entry.retryTimer != 0) {
			context.owner().cancelTimer(entry.retryTimer);
			entry.retryTimer = 0;
		}
		if (entries.isEmpty())
			startGrace();
	}

	private void retryLater(Entry entry) {
		if (entry.retryTimer != 0 || stopped || replaced)
			return;
		entry.retryTimer = context.owner().setTimer(REFUSED_RETRY_MILLIS, t -> {
			entry.retryTimer = 0;
			if (entries.get(entry.key) == entry && !stopped && !replaced)
				post(List.of(entry)).onSuccess(v -> ensureReader());
		});
	}

	// A local subscription cancelled; the gateway's target goes with the last of them.
	private void release(Sub sub) {
		if (sub.cancelled)
			return;
		sub.cancelled = true;
		sub.ready.tryFail(new IllegalStateException("Subscription cancelled"));

		Entry entry = sub.entry;
		entry.subs.remove(sub);
		if (!entry.subs.isEmpty() || entries.get(entry.key) != entry)
			return;

		if (!stopped && !replaced)
			onContext(transport.unsubscribe(entry.key.kind.value(), entry.key.id))
					.onFailure(e -> log.debug("Unsubscribing {} failed: {}", entry.key.id, e.getMessage()));
		drop(entry);
	}

	private void startGrace() {
		cancelGrace();
		graceTimer = context.owner().setTimer(Math.max(1, gracePeriod), t -> {
			graceTimer = 0;
			if (!entries.isEmpty() || stopped)
				return;
			closeReader();
			lastEventId = null;
			if (!replaced)
				onContext(transport.deleteStream())
						.onFailure(e -> log.debug("Deleting the subscription stream failed: {}", e.getMessage()));
		});
	}

	private void cancelGrace() {
		if (graceTimer != 0) {
			context.owner().cancelTimer(graceTimer);
			graceTimer = 0;
		}
	}

	// ---- The event stream -----------------------------------------------------------------------

	private void ensureReader() {
		if (reader != null || connecting || reconnectTimer != 0 || stopped || replaced || entries.isEmpty())
			return;
		connect();
	}

	private void connect() {
		connecting = true;
		onContext(transport.openStream(lastEventId)).onComplete(ar -> {
			connecting = false;
			if (stopped || replaced) {
				if (ar.succeeded())
					ar.result().request().reset();
				return;
			}
			if (ar.failed()) {
				log.debug("Subscription stream failed to open: {}", ar.cause().getMessage());
				scheduleReconnect(0);
				return;
			}

			HttpClientResponse response = ar.result();
			int status = response.statusCode();
			if (status == 200) {
				opened(response);
			} else if (status == 404) {
				// The gateway no longer has the stream: subscribe everything again, then read the new one -
				// at once the first time, with the usual backoff should it be gone again.
				response.body();
				lastEventId = null;
				post(new ArrayList<>(entries.values())).onComplete(v -> {
					if (attempts++ == 0)
						ensureReader();
					else
						scheduleReconnect(0);
				});
			} else {
				response.body();
				String retryAfter = response.getHeader("Retry-After");
				long delay = 0;
				try {
					delay = retryAfter != null ? Long.parseLong(retryAfter.trim()) * 1000 : 0;
				} catch (NumberFormatException ignore) {
					// Not a number of seconds: back off as usual.
				}
				log.debug("Subscription stream answered {}", status);
				scheduleReconnect(delay);
			}
		});
	}

	private void opened(HttpClientResponse response) {
		reader = response;
		attempts = 0;
		lastBytesAt = MonotonicClock.elapsedMillis();
		SseParser parser = new SseParser(new SseParser.Handler() {
			@Override
			public void onEvent(String name, String data, @Nullable String id) {
				if (reader == response)
					onStreamEvent(name, data);
			}

			@Override
			public void onRetry(long millis) {
				retryMillis = Math.max(500, Math.min(millis, MAX_BACKOFF_MILLIS));
			}
		});

		response.handler(chunk -> {
			if (reader != response)
				return;
			lastBytesAt = MonotonicClock.elapsedMillis();
			try {
				parser.feed(chunk.getBytes());
				lastEventId = parser.lastEventId() != null ? parser.lastEventId() : lastEventId;
			} catch (RuntimeException e) {
				log.warn("Malformed subscription stream: {}", e.getMessage());
				dropped(response);
			}
		});
		response.endHandler(v -> dropped(response));
		response.exceptionHandler(e -> dropped(response));
		response.resume();

		watchdogTimer = context.owner().setPeriodic(SILENCE_MILLIS / 3, SILENCE_MILLIS / 3, t -> {
			if (reader == response && MonotonicClock.elapsedMillis() - lastBytesAt > SILENCE_MILLIS) {
				log.debug("Subscription stream silent for {}ms; reconnecting", SILENCE_MILLIS);
				dropped(response);
			}
		});

		// What the stream replays does not repeat a status that has not changed: restore it.
		for (Entry entry : entries.values()) {
			if (entry.subscribedNodes >= 0) {
				for (Sub sub : new ArrayList<>(entry.subs))
					sub.status(entry.subscribedNodes);
			}
		}
	}

	private void dropped(HttpClientResponse response) {
		if (reader != response)
			return;
		closeReader();
		for (Entry entry : entries.values()) {
			for (Sub sub : new ArrayList<>(entry.subs))
				sub.status(0);
		}
		scheduleReconnect(0);
	}

	private void closeReader() {
		if (watchdogTimer != 0) {
			context.owner().cancelTimer(watchdogTimer);
			watchdogTimer = 0;
		}
		HttpClientResponse response = reader;
		reader = null;
		if (response != null)
			response.request().reset();
	}

	private void scheduleReconnect(long delayMillis) {
		if (reconnectTimer != 0 || stopped || replaced || entries.isEmpty())
			return;
		long backoff = Math.min(MAX_BACKOFF_MILLIS, retryMillis << Math.min(attempts, 5));
		long delay = Math.max(delayMillis, backoff) + ThreadLocalRandom.current().nextLong(1 + backoff / 4);
		attempts++;
		reconnectTimer = context.owner().setTimer(delay, t -> {
			reconnectTimer = 0;
			ensureReader();
		});
	}

	private void onStreamEvent(String name, String data) {
		JsonObject json;
		try {
			json = new JsonObject(data);
		} catch (RuntimeException e) {
			log.warn("Malformed subscription event {}: {}", name, e.getMessage());
			return;
		}

		switch (name) {
			case "update" -> onUpdate(json);
			case "status" -> {
				Entry entry = entryOf(json);
				if (entry != null) {
					entry.subscribedNodes = json.getInteger("subscribedNodes", 0);
					for (Sub sub : new ArrayList<>(entry.subs))
						sub.status(entry.subscribedNodes);
				}
			}
			case "refused" -> {
				Entry entry = entryOf(json);
				if (entry != null)
					refused(entry, new HiggsException(403, "Subscription refused: " + json.getString("reason")));
			}
			case "closing" -> onClosing(json.getString("reason", ""));
			default -> {
				// resync, and events this client does not know: nothing to do.
			}
		}
	}

	private @Nullable Entry entryOf(JsonObject json) {
		Kind kind = "value".equals(json.getString("kind")) ? Kind.VALUE :
				"peer".equals(json.getString("kind")) ? Kind.PEER : null;
		String id = json.getString("id");
		if (kind == null || id == null)
			return null;
		try {
			return entries.get(new Key(kind, Id.of(id)));
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	private void onUpdate(JsonObject json) {
		Entry entry = entryOf(json);
		JsonObject record = json.getJsonObject("record");
		if (entry == null || record == null)
			return;

		try {
			if (entry.key.kind == Kind.VALUE) {
				Value value = record.mapTo(Value.class);
				if (!value.getId().equals(entry.key.id) || !value.isValid()) {
					log.warn("Dropped an invalid value for {} from the gateway", entry.key.id);
					return;
				}
				Value latest = entry.latestValue;
				if (latest == null || value.getSequenceNumber() > latest.getSequenceNumber())
					entry.latestValue = value;
				for (Sub sub : new ArrayList<>(entry.subs))
					sub.deliver(value);
			} else {
				PeerInfo peer = record.mapTo(PeerInfo.class);
				if (!peer.getId().equals(entry.key.id) || !peer.isValid()) {
					log.warn("Dropped an invalid peer record for {} from the gateway", entry.key.id);
					return;
				}
				PeerInfo known = entry.latestPeers.get(peer.getFingerprint());
				if (known == null || peer.getSequenceNumber() > known.getSequenceNumber())
					entry.latestPeers.put(peer.getFingerprint(), peer);
				for (Sub sub : new ArrayList<>(entry.subs))
					sub.deliver(peer);
			}
		} catch (RuntimeException e) {
			log.warn("Malformed record for {} from the gateway: {}", entry.key.id, e.getMessage());
		}
	}

	private void onClosing(String reason) {
		HttpClientResponse response = reader;
		if (response == null)
			return;

		if ("replaced".equals(reason)) {
			// Another client reads this device's stream: two clients must not share a device, and the
			// newer one keeps it. Reconnecting would only take it back and forth.
			log.warn("Another client is using this device's subscription stream; this node's subscriptions end");
			replaced = true;
			closeReader();
			HiggsException error = replacedError();
			for (Entry entry : entries.values()) {
				for (Sub sub : new ArrayList<>(entry.subs)) {
					sub.ready.tryFail(error);
					sub.status(0);
				}
			}
			return;
		}

		// deleted or expired by the gateway while targets remain, reauth, shutdown: read again; a stream
		// that is gone answers 404 and is subscribed again.
		dropped(response);
	}

	private static HiggsException replacedError() {
		return new HiggsException(409, "Another client is using this device's subscriptions: each client needs a device of its own");
	}

	// Answers on this class's context, whichever the transport answers on.
	private <T> Future<T> onContext(Future<T> future) {
		Promise<T> promise = Promise.promise();
		future.onComplete(ar -> context.runOnContext(v -> promise.handle(ar)));
		return promise.future();
	}

	/**
	 * Ends every subscription and deletes the stream.
	 *
	 * @return when the gateway has been told, or failed to be
	 */
	Future<Void> stop() {
		return ContextualFuture.callOnContext(context, () -> {
			if (stopped)
				return Future.<Void>succeededFuture();
			stopped = true;
			cancelGrace();
			if (reconnectTimer != 0) {
				context.owner().cancelTimer(reconnectTimer);
				reconnectTimer = 0;
			}
			for (Entry entry : entries.values()) {
				if (entry.retryTimer != 0)
					context.owner().cancelTimer(entry.retryTimer);
				for (Sub sub : entry.subs) {
					sub.cancelled = true;
					sub.ready.tryFail(new IllegalStateException("Node stopped"));
				}
			}
			boolean hadStream = !entries.isEmpty() || reader != null;
			entries.clear();
			closeReader();
			if (!hadStream || replaced)
				return Future.<Void>succeededFuture();
			return onContext(transport.deleteStream()).<Void>mapEmpty().recover(e -> Future.succeededFuture());
		});
	}
}
