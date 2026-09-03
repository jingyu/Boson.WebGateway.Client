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

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import io.bosonnetwork.Id;
import io.bosonnetwork.crypto.Signature;

/**
 * The gateway is not always mounted at the root of its origin: behind a reverse proxy it is
 * commonly published under a path prefix, with the proxy stripping the prefix before the request
 * reaches the service. The client used to ignore whatever path the gateway URL carried and request
 * {@code /v1/...} regardless, which is why it worked when talking to a gateway directly and 404'd
 * behind the proxy. These tests pin the derived request path, since a gateway URL with a path is
 * the case that no other test in the tree covers.
 */
@ExtendWith(VertxExtension.class)
public class GatewayBasePathTests {
	/** Every path the stub gateway was asked for, in order. */
	private final List<String> requestedPaths = new ArrayList<>();

	/**
	 * A stub gateway that records the paths it is asked for and answers {@code /info} - whatever
	 * prefix that lands under - with an identity the client will accept.
	 */
	private Future<HttpServer> startStubGateway(Vertx vertx, Id peerId, Id nodeId) {
		return vertx.createHttpServer()
				.requestHandler(req -> {
					requestedPaths.add(req.path());
					if (req.path().endsWith("/info"))
						req.response()
								.putHeader("Content-Type", "application/json")
								.end(new JsonObject()
										.put("peerId", peerId.toString())
										.put("nodeId", nodeId.toString())
										.put("version", "Orca/1")
										.encode());
					else
						req.response().setStatusCode(404).end("Not Found");
				})
				.listen(0, "127.0.0.1");
	}

	private HiggsNode client(Vertx vertx, Id peerId, String gatewayUrl) {
		return HiggsNode.builder()
				.vertx(vertx)
				.userKey(Signature.KeyPair.random())
				.deviceKey(Signature.KeyPair.random())
				.gatewayPeerId(peerId)
				.gatewayUrl(gatewayUrl)
				.build();
	}

	private void checkFirstRequestPath(Vertx vertx, VertxTestContext context, String pathSuffix, String expectedPath) {
		Id peerId = Id.random();
		Id nodeId = Id.random();

		startStubGateway(vertx, peerId, nodeId)
				.compose(server -> {
					String url = "http://127.0.0.1:" + server.actualPort() + pathSuffix;
					HiggsNode node = client(vertx, peerId, url);
					return Future.fromCompletionStage(node.start())
							.eventually(() -> Future.fromCompletionStage(node.stop()).otherwiseEmpty())
							.eventually(server::close);
				})
				.onComplete(context.succeeding(v -> {
					context.verify(() -> assertEquals(List.of(expectedPath), requestedPaths));
				}))
				.onComplete(context.succeedingThenComplete());
	}

	@Test
	void testGatewayMountedAtRoot(Vertx vertx, VertxTestContext context) {
		checkFirstRequestPath(vertx, context, "", "/v1/info");
	}

	@Test
	void testGatewayMountedAtRootWithTrailingSlash(Vertx vertx, VertxTestContext context) {
		checkFirstRequestPath(vertx, context, "/", "/v1/info");
	}

	@Test
	void testGatewayMountedUnderPathPrefix(Vertx vertx, VertxTestContext context) {
		checkFirstRequestPath(vertx, context, "/gateway", "/gateway/v1/info");
	}

	@Test
	void testGatewayMountedUnderPathPrefixWithTrailingSlash(Vertx vertx, VertxTestContext context) {
		checkFirstRequestPath(vertx, context, "/gateway/", "/gateway/v1/info");
	}

	@Test
	void testRedundantTrailingSlashesAreCollapsed(Vertx vertx, VertxTestContext context) {
		// A configured URL is typed by hand, so it can end in more slashes than anyone intended.
		checkFirstRequestPath(vertx, context, "/gateway//", "/gateway/v1/info");
	}

	@Test
	void testGatewayMountedUnderNestedPathPrefix(Vertx vertx, VertxTestContext context) {
		// The form the config template advertises: https://example.com/boson/gateway
		checkFirstRequestPath(vertx, context, "/boson/gateway", "/boson/gateway/v1/info");
	}
}
