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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

public class SseParserTests {
	private record Event(String name, String data, @Nullable String id) {
	}

	private static final class Recorder implements SseParser.Handler {
		final List<Event> events = new ArrayList<>();
		final List<Long> retries = new ArrayList<>();

		@Override
		public void onEvent(String name, String data, @Nullable String id) {
			events.add(new Event(name, data, id));
		}

		@Override
		public void onRetry(long millis) {
			retries.add(millis);
		}
	}

	private static Recorder parse(String... chunks) {
		Recorder recorder = new Recorder();
		SseParser parser = new SseParser(recorder);
		for (String chunk : chunks)
			parser.feed(chunk.getBytes(StandardCharsets.UTF_8));
		return recorder;
	}

	@Test
	void anEventWithAllItsFields() {
		Recorder r = parse("retry: 3000\n\nid: 7\nevent: update\ndata: {\"a\":1}\n\n");
		assertEquals(List.of(3000L), r.retries);
		assertEquals(List.of(new Event("update", "{\"a\":1}", "7")), r.events);
	}

	@Test
	void dataLinesAreJoinedWithLf() {
		Recorder r = parse("data: one\ndata: two\ndata\n\n");
		assertEquals(List.of(new Event("message", "one\ntwo\n", null)), r.events);
	}

	@Test
	void linesMayEndInCrlfOrCr() {
		Recorder r = parse("event: a\r\ndata: 1\r\n\r\nevent: b\rdata: 2\r\r");
		assertEquals(List.of(new Event("a", "1", null), new Event("b", "2", null)), r.events);
	}

	@Test
	void aCrlfSplitAcrossChunksIsOneLineEnd() {
		Recorder r = parse("data: 1\r", "\n\r", "\n");
		assertEquals(List.of(new Event("message", "1", null)), r.events);
	}

	@Test
	void anEventSplitAnywhereAcrossChunks() {
		String stream = "id: 42\nevent: status\ndata: {\"subscribedNodes\":3}\n\n";
		for (int split = 1; split < stream.length(); split++) {
			Recorder r = parse(stream.substring(0, split), stream.substring(split));
			assertEquals(List.of(new Event("status", "{\"subscribedNodes\":3}", "42")), r.events, "split at " + split);
		}
	}

	@Test
	void aMultiByteCharacterSplitAcrossChunks() {
		byte[] bytes = "data: café\n\n".getBytes(StandardCharsets.UTF_8);
		Recorder recorder = new Recorder();
		SseParser parser = new SseParser(recorder);
		int split = bytes.length - 3;   // inside the two bytes of the accented letter
		byte[] head = new byte[split];
		byte[] tail = new byte[bytes.length - split];
		System.arraycopy(bytes, 0, head, 0, split);
		System.arraycopy(bytes, split, tail, 0, tail.length);
		parser.feed(head);
		parser.feed(tail);
		assertEquals(List.of(new Event("message", "café", null)), recorder.events);
	}

	@Test
	void commentsAndHeartbeatsDispatchNothing() {
		Recorder r = parse(":\n\n: keep-alive\n\n");
		assertEquals(List.of(), r.events);
	}

	@Test
	void anEventWithoutDataIsNotDispatchedAndItsNameDoesNotCarryOver() {
		Recorder r = parse("event: closing\n\ndata: x\n\n");
		assertEquals(List.of(new Event("message", "x", null)), r.events);
	}

	@Test
	void theLastEventIdCarriesOverUntilChanged() {
		Recorder recorder = new Recorder();
		SseParser parser = new SseParser(recorder);
		parser.feed("id: 1\ndata: a\n\ndata: b\n\nid\ndata: c\n\n".getBytes(StandardCharsets.UTF_8));
		assertEquals(List.of(new Event("message", "a", "1"), new Event("message", "b", "1"),
				new Event("message", "c", "")), recorder.events);
		assertEquals("", parser.lastEventId());
	}

	@Test
	void anIdWithNulIsIgnored() {
		Recorder recorder = new Recorder();
		SseParser parser = new SseParser(recorder);
		parser.feed("id: 5\ndata: a\n\nid: 6\u0000\ndata: b\n\n".getBytes(StandardCharsets.UTF_8));
		assertEquals("5", recorder.events.get(1).id());
		assertEquals("5", parser.lastEventId());
	}

	@Test
	void retryMustBeDigits() {
		Recorder r = parse("retry: 1x\nretry: -5\nretry:\nretry: 99999999999999999999999\nretry: 250\n\n");
		assertEquals(List.of(250L), r.retries);
	}

	@Test
	void onlyOneLeadingSpaceIsDropped() {
		Recorder r = parse("data:no space\n\ndata:  two spaces\n\n");
		assertEquals(List.of(new Event("message", "no space", null), new Event("message", " two spaces", null)),
				r.events);
	}

	@Test
	void unknownFieldsAreIgnored() {
		Recorder r = parse("foo: bar\ndata: x\n\n");
		assertEquals(List.of(new Event("message", "x", null)), r.events);
	}

	@Test
	void anUnfinishedEventIsNotDispatched() {
		Recorder recorder = new Recorder();
		SseParser parser = new SseParser(recorder);
		parser.feed("data: partial\n".getBytes(StandardCharsets.UTF_8));
		assertEquals(List.of(), recorder.events);
		assertNull(parser.lastEventId());
	}

	@Test
	void anOverlongLineIsRefused() {
		SseParser parser = new SseParser(new Recorder());
		byte[] chunk = new byte[64 * 1024];
		Arrays.fill(chunk, (byte) 'a');
		assertThrows(IllegalStateException.class, () -> {
			for (int i = 0; i < 17; i++)
				parser.feed(chunk);
		});
	}
}
