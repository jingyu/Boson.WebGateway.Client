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
 * Exceptions thrown by the Higgs gateway client.
 * <p>
 * {@link HiggsException} is the base type. Each error the gateway reports is mapped to a dedicated
 * subclass, so callers can react by catching the specific type instead of inspecting a status code:
 * <ul>
 *   <li>{@link InvalidRequestException} - HTTP {@code 400}</li>
 *   <li>{@link UnauthorizedException} - HTTP {@code 401}</li>
 *   <li>{@link ForbiddenException} - HTTP {@code 403}, covering both an authorization failure and
 *       a persistent-storage quota being exhausted</li>
 *   <li>{@link NotFoundException} - HTTP {@code 404}</li>
 *   <li>{@link PreconditionFailedException} - HTTP {@code 412}</li>
 *   <li>{@link RateLimitException} - HTTP {@code 429}</li>
 *   <li>{@link ServiceBusyException} - HTTP {@code 503}</li>
 *   <li>{@link GatewayTimeoutException} - HTTP {@code 504}</li>
 *   <li>{@link GatewayServerException} - HTTP {@code 500} and other {@code 5xx}</li>
 * </ul>
 * A transport- or client-side failure with no HTTP response (connection failure, TLS error,
 * malformed gateway response) surfaces as a plain {@code HiggsException} whose status is
 * {@link HiggsException#NO_HTTP_STATUS}, as does any status the client does not recognize.
 *
 * <h2>Retrying</h2>
 * Three of these are worth retrying and the rest are not, which is the main reason to catch by type.
 * {@link RateLimitException} and {@link ServiceBusyException} both carry a {@code getRetryAfter()}
 * delay in seconds taken from the gateway's {@code Retry-After} header - honour it rather than
 * retrying immediately, since an immediate retry is certain to fail and a fleet of clients doing it
 * turns a throttle into a retry storm. They differ in what the caller should conclude: a rate limit
 * means this client consumed a budget and should slow down, whereas a busy gateway means the node
 * shed work and the client's own rate is not the problem.
 * <p>
 * {@link PreconditionFailedException} is retryable only after re-reading the value and re-applying
 * the change on top of what is now stored; repeating the same request unchanged fails identically.
 *
 * @see io.bosonnetwork.higgs.HiggsNode
 * @see HiggsException
 */
@NullMarked
package io.bosonnetwork.higgs.exceptions;

import org.jspecify.annotations.NullMarked;