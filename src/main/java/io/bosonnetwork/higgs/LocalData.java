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

import io.bosonnetwork.Node;
import io.bosonnetwork.PeerInfo;
import io.bosonnetwork.Value;

public class LocalData<T> {
	private final T data;
	private final boolean persistent;
	private final long lastAnnounced;
	private final long ttl;

	protected LocalData(T data, boolean persistent) {
		this.data = data;
		this.persistent = persistent;
		this.lastAnnounced = System.currentTimeMillis();

		if (data instanceof PeerInfo)
			ttl = Node.MAX_PEER_AGE;
		else if (data instanceof Value)
			ttl = Node.MAX_VALUE_AGE;
		else
			throw new IllegalArgumentException("Unsupported data type: " + data.getClass());
	}

	protected LocalData(T data) {
		this(data, false);
	}

	public T data() {
		return data;
	}

	public boolean isPersistent() {
		return persistent;
	}

	public long lastAnnounced() {
		return lastAnnounced;
	}

	public boolean isExpired() {
		return !persistent && System.currentTimeMillis() - lastAnnounced > ttl;
	}
}