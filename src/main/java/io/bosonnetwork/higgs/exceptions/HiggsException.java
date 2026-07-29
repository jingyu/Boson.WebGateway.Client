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

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import org.jspecify.annotations.Nullable;

import io.bosonnetwork.BosonException;
import io.bosonnetwork.higgs.HiggsNode;

/**
 * Base type for failures reported by the {@link HiggsNode} gateway client.
 * <p>
 * It carries an optional HTTP {@linkplain #getStatus() status code}. When the failure
 * corresponds to an HTTP error response from the gateway, the status is that response's
 * code (e.g. {@code 403}, {@code 429}, {@code 412}) and the message is the response body.
 * When the failure is a transport- or client-side error with no HTTP response (connection
 * failure, TLS error, malformed gateway response), the status is {@link #NO_HTTP_STATUS}
 * ({@code 0}) and the original error is available via {@link #getCause()}.
 *
 * <h2>Catching by type</h2>
 * Each error the gateway reports is mapped by {@link #fromResponse} to a dedicated subclass, so
 * callers can react by catching the specific type rather than branching on a status code. The
 * distinctions that matter most in practice are the ones a caller can actually do something about:
 * {@link RateLimitException} and {@link ServiceBusyException} are worth retrying after the delay
 * they carry, {@link PreconditionFailedException} means re-read and retry with a fresh sequence
 * number, and {@link UnauthorizedException} means the token needs renewing - while
 * {@link InvalidRequestException} means the request will never succeed as written.
 *
 * <h2>Why classification is by status alone</h2>
 * Unlike the Ion Store, the Web Gateway returns {@code text/plain} error bodies with no structured
 * type or code, so the HTTP status is the only machine-readable signal available. That is enough
 * for every case except one: the gateway uses {@code 403} both for an authorization failure and for
 * a persistent-storage quota being exhausted. Those two mean quite different things to a caller, but
 * telling them apart would mean matching on message text, which breaks silently the first time the
 * wording changes. Both therefore surface as {@link ForbiddenException}; see its documentation.
 */
public class HiggsException extends BosonException {
	private static final long serialVersionUID = 601272866364670520L;

	/**
	 * Sentinel {@linkplain #getStatus() status} value indicating that the failure has no
	 * associated HTTP status code (i.e. a transport- or client-side error).
	 */
	public static final int NO_HTTP_STATUS = 0;

	/**
	 * Sentinel {@linkplain RateLimitException#getRetryAfter() retry-after} value indicating that the
	 * response carried no usable {@code Retry-After} header.
	 */
	public static final long NO_RETRY_AFTER = 0;

	private final int status;

	/**
	 * Creates an exception for an HTTP error response.
	 *
	 * @param status  the HTTP status code returned by the gateway
	 * @param message the detail message (typically the response body)
	 */
	public HiggsException(int status, @Nullable String message) {
		super(message);
		this.status = status;
	}

	/**
	 * Creates an exception for an HTTP error response, with an underlying cause.
	 *
	 * @param status  the HTTP status code returned by the gateway
	 * @param message the detail message
	 * @param cause   the underlying cause
	 */
	public HiggsException(int status, @Nullable String message, @Nullable Throwable cause) {
		super(message, cause);
		this.status = status;
	}

	/**
	 * Creates an exception for a transport- or client-side failure with no HTTP status
	 * (status is set to {@link #NO_HTTP_STATUS}).
	 *
	 * @param message the detail message
	 * @param cause   the underlying cause
	 */
	public HiggsException(@Nullable String message, @Nullable Throwable cause) {
		super(message, cause);
		this.status = NO_HTTP_STATUS;
	}

	/**
	 * Creates an exception for a transport- or client-side failure with no HTTP status
	 * (status is set to {@link #NO_HTTP_STATUS}).
	 *
	 * @param cause the underlying cause
	 */
	public HiggsException(@Nullable Throwable cause) {
		super(cause);
		this.status = NO_HTTP_STATUS;
	}

	/**
	 * Returns the HTTP status code associated with this failure, or {@link #NO_HTTP_STATUS}
	 * ({@code 0}) if the failure was a transport- or client-side error with no HTTP response.
	 *
	 * @return the HTTP status code, or {@link #NO_HTTP_STATUS} if none
	 */
	public int getStatus() {
		return status;
	}

	/**
	 * Builds an exception from a gateway error response, mapping it to the most specific subclass.
	 * <p>
	 * A status the client does not recognize surfaces as a plain {@code HiggsException} that still
	 * preserves the status and the response body, so a gateway that starts reporting something new
	 * degrades to the old behaviour rather than being mistaken for a status it is not.
	 *
	 * @param response the gateway's error response
	 * @return a classified {@code HiggsException}, or a subclass thereof
	 */
	public static HiggsException fromResponse(HttpResponse<Buffer> response) {
		return of(response.statusCode(), response.bodyAsString(),
				parseRetryAfter(response.getHeader("Retry-After")));
	}

	/**
	 * Maps a status, message and retry hint to the most specific exception type.
	 * <p>
	 * Kept free of any HTTP type so the classification can be exercised - and reused - without
	 * standing up a response object. {@link #fromResponse} is the normal entry point.
	 *
	 * @param status     the HTTP status code returned by the gateway
	 * @param message    the detail message, typically the response body
	 * @param retryAfter seconds to wait before retrying, or {@link #NO_RETRY_AFTER} if unknown;
	 *                   ignored for statuses that are not retryable
	 * @return a classified {@code HiggsException}, or a subclass thereof
	 */
	public static HiggsException of(int status, @Nullable String message, long retryAfter) {
		return switch (status) {
			case 400 -> new InvalidRequestException(status, message);
			case 401 -> new UnauthorizedException(status, message);
			case 403 -> new ForbiddenException(status, message);
			case 404 -> new NotFoundException(status, message);
			case 412 -> new PreconditionFailedException(status, message);
			case 429 -> new RateLimitException(status, message, retryAfter);
			case 503 -> new ServiceBusyException(status, message, retryAfter);
			case 504 -> new GatewayTimeoutException(status, message);
			default -> status >= 500
					? new GatewayServerException(status, message)
					: new HiggsException(status, message);
		};
	}

	/**
	 * Parses a {@code Retry-After} header value as a number of seconds.
	 * <p>
	 * Only the delta-seconds form is understood; the HTTP-date form is not, and neither is a missing
	 * or unparseable value. All of those yield {@link #NO_RETRY_AFTER} rather than an exception,
	 * because a client that cannot read the hint should back off on its own schedule rather than
	 * fail differently than it would have. A non-positive value is treated the same way: reporting
	 * "retry in zero seconds" would invite exactly the immediate retry the header exists to prevent.
	 *
	 * @param value the raw header value, may be {@code null}
	 * @return the delay in seconds, or {@link #NO_RETRY_AFTER} when absent or unusable
	 */
	public static long parseRetryAfter(@Nullable String value) {
		if (value == null || value.isBlank())
			return NO_RETRY_AFTER;

		try {
			long seconds = Long.parseLong(value.trim());
			return seconds > 0 ? seconds : NO_RETRY_AFTER;
		} catch (NumberFormatException e) {
			return NO_RETRY_AFTER;
		}
	}
}