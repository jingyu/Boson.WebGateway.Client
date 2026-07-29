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

package io.bosonnetwork.higgs.exceptions;

import org.jspecify.annotations.Nullable;

/**
 * Thrown when a DHT operation did not complete within the gateway's timeout; HTTP {@code 504}.
 * <p>
 * The request was accepted and the work was attempted - it simply did not finish in time, typically
 * because the DHT lookup found no responsive nodes. A store or announce that times out may still
 * have taken effect, so treat this as an indeterminate outcome rather than a definite failure, and
 * prefer re-reading over blindly repeating a write.
 */
public class GatewayTimeoutException extends HiggsException {
	private static final long serialVersionUID = -1478234968403395108L;

	/**
	 * Creates a {@code GatewayTimeoutException} from a gateway error response.
	 *
	 * @param status  the HTTP status code returned by the gateway
	 * @param message the detail message, typically the response body
	 */
	public GatewayTimeoutException(int status, @Nullable String message) {
		super(status, message);
	}
}