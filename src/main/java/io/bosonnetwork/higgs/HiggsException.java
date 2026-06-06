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

import io.bosonnetwork.BosonException;

/**
 * Exception thrown by the {@link HiggsNode} gateway client.
 * <p>
 * It carries an optional HTTP {@linkplain #getStatus() status code}. When the failure
 * corresponds to an HTTP error response from the gateway, the status is that response's
 * code (e.g. {@code 403}, {@code 429}, {@code 412}) and the message is the response body.
 * When the failure is a transport- or client-side error with no HTTP response (connection
 * failure, TLS error, malformed gateway response), the status is {@link #NO_HTTP_STATUS}
 * ({@code 0}) and the original error is available via {@link #getCause()}.
 */
public class HiggsException extends BosonException {
	private static final long serialVersionUID = 601272866364670520L;

	/**
	 * Sentinel {@linkplain #getStatus() status} value indicating that the failure has no
	 * associated HTTP status code (i.e. a transport- or client-side error).
	 */
	public static final int NO_HTTP_STATUS = 0;

	private final int status;

	/**
	 * Creates an exception for an HTTP error response.
	 *
	 * @param status  the HTTP status code returned by the gateway
	 * @param message the detail message (typically the response body)
	 */
	public HiggsException(int status, String message) {
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
	public HiggsException(int status, String message, Throwable cause) {
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
	public HiggsException(String message, Throwable cause) {
		super(message, cause);
		this.status = NO_HTTP_STATUS;
	}

	/**
	 * Creates an exception for a transport- or client-side failure with no HTTP status
	 * (status is set to {@link #NO_HTTP_STATUS}).
	 *
	 * @param cause the underlying cause
	 */
	public HiggsException(Throwable cause) {
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
}