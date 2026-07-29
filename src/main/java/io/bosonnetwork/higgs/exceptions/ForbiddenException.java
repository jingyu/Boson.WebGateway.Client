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
 * Thrown when the gateway refuses an otherwise well-formed request; HTTP {@code 403}.
 * <p>
 * This covers two situations the gateway does not currently distinguish on the wire, because its
 * error bodies are plain text with no structured type or code:
 * <ul>
 *   <li><b>Authorization failure</b> - the token authenticated but does not grant the {@code client}
 *       role for this operation.</li>
 *   <li><b>Persistent storage quota exhausted</b> - the caller is at their plan's
 *       {@code maxPersistentValues} or {@code maxPersistentPeers} limit. Removing something, or
 *       moving to a larger plan, is what resolves it.</li>
 * </ul>
 * The two are told apart only by {@linkplain #getMessage() message} text today. Deliberately not
 * parsed into separate types here: matching on wording would break silently the first time the
 * gateway rephrases a message, which is worse than making the caller read it. Splitting this
 * properly needs a structured error body from the service, as the Ion Store already has.
 */
public class ForbiddenException extends HiggsException {
	private static final long serialVersionUID = 2273669283900841171L;

	/**
	 * Creates a {@code ForbiddenException} from a gateway error response.
	 *
	 * @param status  the HTTP status code returned by the gateway
	 * @param message the detail message, typically the response body
	 */
	public ForbiddenException(int status, @Nullable String message) {
		super(status, message);
	}
}