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

/**
 * Higgs — a lightweight Boson {@link io.bosonnetwork.Node Node} client backed by a Web Gateway.
 * <p>
 * This package provides a thin client that implements the {@link io.bosonnetwork.Node} API by
 * talking to a Boson Web Gateway over its authenticated HTTP/REST interface, instead of joining
 * the DHT directly. It is intended for applications (mobile, CLI, embedded, browser-adjacent)
 * that need DHT access but cannot or should not run a full node: it keeps no routing table and no
 * local storage, and is cheap to create and tear down.
 *
 * <h2>Entry point</h2>
 * {@link io.bosonnetwork.higgs.HiggsNode} is the entry point, constructed through its
 * {@linkplain io.bosonnetwork.higgs.HiggsNode#builder() builder}. Beyond the standard
 * {@code Node} operations (find/store value, find/announce peer), it also exposes the gateway's
 * per-user persistent storage (list/get/remove of the caller's own values and peers).
 *
 * <h2>Authentication</h2>
 * Requests are authenticated with short-lived {@link io.bosonnetwork.cwt.SignedCwt CWT} bearer
 * tokens, minted in one of two mutually exclusive modes:
 * <ul>
 *   <li><b>User-key mode</b> — the client signs tokens directly as the user.</li>
 *   <li><b>Device mode</b> — the client signs as a device acting on behalf of a user.</li>
 * </ul>
 *
 * <h2>Gateway binding</h2>
 * A client is bound to a specific gateway by its node id, peer id and URL; on
 * {@linkplain io.bosonnetwork.higgs.HiggsNode#start() start} the client verifies the gateway's
 * advertised identity against those values (and, over HTTPS, pins the gateway's self-signed
 * certificate to its peer id) before any request is issued.
 *
 * <h2>Errors</h2>
 * Failures are reported as {@link io.bosonnetwork.higgs.HiggsException}, which carries the HTTP
 * status of a gateway error response, or {@link io.bosonnetwork.higgs.HiggsException#NO_HTTP_STATUS}
 * for transport- or client-side failures.
 *
 * @see io.bosonnetwork.higgs.HiggsNode
 * @see io.bosonnetwork.higgs.HiggsException
 */
package io.bosonnetwork.higgs;
