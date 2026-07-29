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
 * Thrown when a rate limit has been exceeded; HTTP {@code 429}.
 * <p>
 * The gateway enforces per-user, per-address and service-wide budgets together and does not say
 * which one refused. All three refill continuously rather than at a clock boundary, so waiting the
 * {@linkplain #getRetryAfter() advertised delay} is enough - there is no window to wait out.
 * <p>
 * Distinct from {@link ServiceBusyException}: this one means the caller consumed a budget, so a
 * client that keeps hitting it should slow down rather than simply retry harder.
 */
public class RateLimitException extends HiggsException {
	private static final long serialVersionUID = -3211900572857834162L;

	private final long retryAfter;

	/**
	 * Creates a {@code RateLimitException} from a gateway error response.
	 *
	 * @param status     the HTTP status code returned by the gateway
	 * @param message    the detail message, typically the response body
	 * @param retryAfter seconds to wait before retrying, or {@link #NO_RETRY_AFTER} if the response
	 *                   carried no usable {@code Retry-After} header
	 */
	public RateLimitException(int status, @Nullable String message, long retryAfter) {
		super(status, message);
		this.retryAfter = retryAfter;
	}

	/**
	 * Creates a {@code RateLimitException} with no retry hint.
	 *
	 * @param status  the HTTP status code returned by the gateway
	 * @param message the detail message, typically the response body
	 */
	public RateLimitException(int status, @Nullable String message) {
		this(status, message, NO_RETRY_AFTER);
	}

	/**
	 * Returns how long to wait before retrying, as reported by the gateway's {@code Retry-After}
	 * header.
	 * <p>
	 * Honour this rather than retrying immediately: an immediate retry is certain to fail, and a fleet of clients that all
	 * retry immediately turns a throttle into a retry storm.
	 *
	 * @return the delay in seconds, or {@link #NO_RETRY_AFTER} ({@code 0}) if the gateway did not
	 *         say. Treat {@code 0} as "back off on your own schedule", never as "retry now".
	 */
	public long getRetryAfter() {
		return retryAfter;
	}
}