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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.jspecify.annotations.Nullable;

/**
 * Reads a {@code text/event-stream}, as the HTML standard defines it, from chunks as they arrive.
 * <p>
 * Lines end in LF, CRLF or CR, also across chunks; a line starting with a colon is a comment; {@code data}
 * lines of one event are joined with LF; a blank line dispatches the event, if it has data. {@code id} sets
 * the last event id (an id with a NUL is ignored), and {@code retry} the reconnection time.
 * </p>
 * <p>
 * Not thread-safe; one instance per connection.
 * </p>
 */
final class SseParser {
	/** What the parser hands over. */
	interface Handler {
		/**
		 * An event.
		 *
		 * @param name the event type, {@code message} when the stream named none
		 * @param data the data
		 * @param id   the last event id after this event, or null if the stream has set none
		 */
		void onEvent(String name, String data, @Nullable String id);

		/**
		 * The reconnection time the stream asks for.
		 *
		 * @param millis milliseconds
		 */
		default void onRetry(long millis) {
		}
	}

	private static final int MAX_LINE = 1 << 20;

	private final Handler handler;
	private final ByteArrayOutputStream line = new ByteArrayOutputStream(256);
	private boolean lastWasCr;

	private @Nullable String eventType;
	private final StringBuilder data = new StringBuilder();
	private boolean hasData;
	private @Nullable String lastEventId;

	SseParser(Handler handler) {
		this.handler = handler;
	}

	/**
	 * Feeds the bytes of the next chunk.
	 *
	 * @param bytes the chunk
	 * @throws IllegalStateException if a line grows past a megabyte
	 */
	void feed(byte[] bytes) {
		for (byte b : bytes) {
			if (b == '\n') {
				if (lastWasCr) {
					lastWasCr = false;
					continue;
				}
				endLine();
			} else if (b == '\r') {
				lastWasCr = true;
				endLine();
			} else {
				lastWasCr = false;
				if (line.size() >= MAX_LINE)
					throw new IllegalStateException("Event stream line too long");
				line.write(b);
			}
		}
	}

	private void endLine() {
		String text = line.toString(StandardCharsets.UTF_8);
		line.reset();

		if (text.isEmpty()) {
			dispatch();
			return;
		}
		if (text.charAt(0) == ':')
			return;

		int colon = text.indexOf(':');
		String field = colon < 0 ? text : text.substring(0, colon);
		String value = colon < 0 ? "" : text.substring(colon + 1);
		if (value.startsWith(" "))
			value = value.substring(1);

		switch (field) {
			case "event" -> eventType = value;
			case "data" -> {
				if (hasData)
					data.append('\n');
				data.append(value);
				hasData = true;
			}
			case "id" -> {
				if (value.indexOf('\0') < 0)
					lastEventId = value;
			}
			case "retry" -> {
				if (!value.isEmpty() && value.chars().allMatch(Character::isDigit)) {
					try {
						handler.onRetry(Long.parseLong(value));
					} catch (NumberFormatException ignore) {
						// Too large to be meant.
					}
				}
			}
			default -> {
				// Unknown fields are ignored.
			}
		}
	}

	private void dispatch() {
		String type = eventType;
		eventType = null;
		if (!hasData)
			return;

		String payload = data.toString();
		data.setLength(0);
		hasData = false;
		handler.onEvent(type == null || type.isEmpty() ? "message" : type, payload, lastEventId);
	}

	/**
	 * The last event id the stream set, for a reconnection's {@code Last-Event-ID}.
	 *
	 * @return the id, or null if none
	 */
	@Nullable String lastEventId() {
		return lastEventId;
	}
}
