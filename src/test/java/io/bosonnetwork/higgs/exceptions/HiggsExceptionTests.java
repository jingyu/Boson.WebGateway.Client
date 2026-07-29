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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;


import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests for the mapping from a gateway error response to a typed exception.
 * <p>
 * The point of the hierarchy is that a caller can decide what to do by catching a type, so these
 * pin the classification and, for the two retryable cases, the retry hint that decision depends on.
 */
@DisplayName("HiggsException classification tests")
class HiggsExceptionTests {
	private static HiggsException classify(int status, String body) {
		return HiggsException.of(status, body, HiggsException.NO_RETRY_AFTER);
	}

	@ParameterizedTest(name = "HTTP {0} -> {1}")
	@CsvSource({
			"400, InvalidRequestException",
			"401, UnauthorizedException",
			"403, ForbiddenException",
			"404, NotFoundException",
			"412, PreconditionFailedException",
			"429, RateLimitException",
			"503, ServiceBusyException",
			"504, GatewayTimeoutException",
			"500, GatewayServerException",
	})
	@DisplayName("Each documented status maps to its own type")
	void testStatusMapsToType(int status, String expected) {
		HiggsException e = classify(status, "boom");

		assertEquals(expected, e.getClass().getSimpleName());
		assertEquals(status, e.getStatus());
		// The body is the only human-readable detail the gateway gives, so it must not be discarded
		// in the course of classifying.
		assertEquals("boom", e.getMessage());
		assertInstanceOf(HiggsException.class, e, "every subclass must remain catchable as the base type");
	}

	/**
	 * A gateway that starts reporting something new must degrade to the old behaviour rather than be
	 * mistaken for a status it is not.
	 */
	@Test
	@DisplayName("An unrecognized status stays a plain HiggsException")
	void testUnknownStatusFallsBack() {
		HiggsException e = classify(418, "teapot");

		assertSame(HiggsException.class, e.getClass());
		assertEquals(418, e.getStatus());
		assertEquals("teapot", e.getMessage());
	}

	/** Any unrecognized 5xx is still a server fault, which is more useful than "unknown". */
	@Test
	@DisplayName("An unrecognized 5xx is a server fault")
	void testUnknownServerErrorIsAServerFault() {
		assertInstanceOf(GatewayServerException.class, classify(502, "bad gateway"));
		assertInstanceOf(GatewayServerException.class, classify(507, "insufficient storage"));
	}

	// -------------------------------------------------------------------------
	// Retry hints
	// -------------------------------------------------------------------------

	@Test
	@DisplayName("Retry-After is carried through on both retryable types")
	void testRetryAfterIsCarried() {
		HiggsException limited = HiggsException.of(429, "Rate limit exceeded",
				HiggsException.parseRetryAfter("42"));
		assertEquals(42, assertInstanceOf(RateLimitException.class, limited).getRetryAfter());

		HiggsException busy = HiggsException.of(503, "Server busy", HiggsException.parseRetryAfter("1"));
		assertEquals(1, assertInstanceOf(ServiceBusyException.class, busy).getRetryAfter());
	}

	/**
	 * A hint the client cannot read must not become a hint to retry immediately. Every unusable form
	 * collapses to {@code NO_RETRY_AFTER}, which callers are documented to treat as "back off on
	 * your own schedule".
	 */
	@ParameterizedTest(name = "Retry-After: {0}")
	@CsvSource(value = {
			"NULL",
			"''",
			"'   '",
			"not-a-number",
			"Wed, 21 Oct 2015 07:28:00 GMT",
			"-5",
			"0",
	}, nullValues = "NULL")
	@DisplayName("An unusable Retry-After yields no hint rather than zero seconds")
	void testUnusableRetryAfter(String header) {
		HiggsException e = HiggsException.of(429, "Rate limit exceeded",
				HiggsException.parseRetryAfter(header));

		assertEquals(HiggsException.NO_RETRY_AFTER,
				assertInstanceOf(RateLimitException.class, e).getRetryAfter());
	}

	/**
	 * The two retryable types must stay distinguishable. They mean different things: a rate limit
	 * says this client consumed a budget and should slow down, a busy gateway says the node shed
	 * work and the client's own rate is not at fault.
	 */
	@Test
	@DisplayName("Rate limited and service busy are not interchangeable")
	void testRetryableTypesAreDistinct() {
		HiggsException limited = classify(429, "Rate limit exceeded");
		HiggsException busy = classify(503, "Server busy");

		assertInstanceOf(RateLimitException.class, limited);
		assertInstanceOf(ServiceBusyException.class, busy);
		assertTrue(!(limited instanceof ServiceBusyException), "429 must not be catchable as ServiceBusyException");
		assertTrue(!(busy instanceof RateLimitException), "503 must not be catchable as RateLimitException");
	}

	// -------------------------------------------------------------------------
	// Backwards compatibility
	// -------------------------------------------------------------------------

	/**
	 * Code written against the old single-type API caught {@code HiggsException} and branched on
	 * {@link HiggsException#getStatus()}. Introducing subclasses must not break it.
	 */
	@Test
	@DisplayName("Existing status-based handling keeps working")
	void testStatusBasedHandlingStillWorks() {
		for (int status : new int[] { 400, 401, 403, 404, 412, 429, 500, 503, 504 }) {
			HiggsException e = classify(status, "boom");
			assertEquals(status, e.getStatus());
			assertNotNull(e.getMessage());
		}
	}

	/** A transport-level failure has no response to classify and keeps its sentinel status. */
	@Test
	@DisplayName("A transport failure carries no HTTP status")
	void testTransportFailure() {
		HiggsException e = new HiggsException("Gateway request failed", new java.io.IOException("connection reset"));

		assertEquals(HiggsException.NO_HTTP_STATUS, e.getStatus());
		assertInstanceOf(java.io.IOException.class, e.getCause());
	}
}