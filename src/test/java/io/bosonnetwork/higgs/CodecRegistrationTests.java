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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;

import org.junit.jupiter.api.Test;

import io.bosonnetwork.AnnounceResult;
import io.bosonnetwork.Id;
import io.bosonnetwork.NodeInfo;
import io.bosonnetwork.crypto.Signature;

/**
 * That a client on its own can read what the gateway sends it.
 * <p>
 * Every response here is decoded through Vert.x's process-wide JSON codec, which knows nothing about
 * Boson types until the module is registered into it. Nothing in this library did that until
 * {@link HiggsNode#start()} began to: the gateway's integration tests pass because the service registers
 * it in the same JVM, so the client side of that arrangement was never actually exercised. A client
 * embedded in an application running no Boson service has nobody to inherit the registration from, and
 * would fail on the first response carrying an {@link Id}.
 * </p>
 */
public class CodecRegistrationTests {
	private static HiggsNode client(Vertx vertx) throws Exception {
		return HiggsNode.builder()
				.vertx(vertx)
				.userId(Id.random())
				.deviceKey(Signature.KeyPair.random())
				.gatewayPeerId(Id.random())
				// Nothing listens here. start() fails at the first request, which is after the point
				// this test is about: the codec has to be usable before a response can be read at all.
				.gatewayUrl(new URL("http://127.0.0.1:1"))
				.build();
	}

	@Test
	void testAStartedClientCanDecodeBosonTypes() throws Exception {
		Vertx vertx = Vertx.vertx();
		try {
			HiggsNode node = client(vertx);
			// Awaited only to be sure start() ran; it fails, and that is expected.
			node.start().toCompletableFuture().handle((v, e) -> null).join();

			Id nodeId = Id.random();
			String json = "{\"targets\":[{\"id\":\"" + nodeId.toBase58String() + "\",\"outcome\":\"acknowledged\"}]}";

			AnnounceResult result = Json.decodeValue(Buffer.buffer(json), AnnounceResult.class);
			assertNotNull(result);
			assertEquals(1, result.targets().size());
			assertEquals(nodeId, result.targets().get(0).nodeId());
			assertTrue(result.isAnnounced());
		} finally {
			vertx.close().toCompletionStage().toCompletableFuture().join();
		}
	}

	@Test
	void testTheSameIsTrueOfTheOtherTypesTheClientReads() throws Exception {
		// AnnounceResult is the newest of them, not a special case: the whole bodyAsJson idiom in this
		// client rests on the same registration.
		Vertx vertx = Vertx.vertx();
		try {
			client(vertx).start().toCompletableFuture().handle((v, e) -> null).join();

			NodeInfo ni = NodeInfo.of(Id.random(), "192.0.2.10", 39001);
			NodeInfo decoded = Json.decodeValue(Buffer.buffer(Json.encode(ni)), NodeInfo.class);
			assertEquals(ni, decoded);
		} finally {
			vertx.close().toCompletionStage().toCompletableFuture().join();
		}
	}
}
