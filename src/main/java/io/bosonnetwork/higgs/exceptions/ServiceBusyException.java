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
 * Thrown when the gateway sheds a request because too many DHT operations are already in flight;
 * HTTP {@code 503}.
 * <p>
 * Unlike {@link RateLimitException} the caller has exceeded nothing - the node declined the work
 * rather than queueing it, so that a throughput problem does not become an unbounded latency
 * problem. Retrying shortly is expected to succeed, and the caller need not reduce its own rate.
 */
public class ServiceBusyException extends HiggsException {
	private static final long serialVersionUID = 8827096145592301274L;

	private final long retryAfter;

	/**
	 * Creates a {@code ServiceBusyException} from a gateway error response.
	 *
	 * @param status     the HTTP status code returned by the gateway
	 * @param message    the detail message, typically the response body
	 * @param retryAfter seconds to wait before retrying, or {@link #NO_RETRY_AFTER} if the response
	 *                   carried no usable {@code Retry-After} header
	 */
	public ServiceBusyException(int status, @Nullable String message, long retryAfter) {
		super(status, message);
		this.retryAfter = retryAfter;
	}

	/**
	 * Creates a {@code ServiceBusyException} with no retry hint.
	 *
	 * @param status  the HTTP status code returned by the gateway
	 * @param message the detail message, typically the response body
	 */
	public ServiceBusyException(int status, @Nullable String message) {
		this(status, message, NO_RETRY_AFTER);
	}

	/**
	 * Returns how long to wait before retrying, as reported by the gateway's {@code Retry-After}
	 * header.
	 * <p>
	 * Honour this rather than retrying immediately: an immediate retry is certain to fail; the node shed this request
	 * on arrival rather than queueing it.
	 *
	 * @return the delay in seconds, or {@link #NO_RETRY_AFTER} ({@code 0}) if the gateway did not
	 *         say. Treat {@code 0} as "back off on your own schedule", never as "retry now".
	 */
	public long getRetryAfter() {
		return retryAfter;
	}
}