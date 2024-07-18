package io.bosonnetwork.higgs;

import io.bosonnetwork.Value;

class PersistentValue {
	private Value value;
	private long lastAnnounced;

	public PersistentValue(Value value) {
		this.value = value;
		this.lastAnnounced = System.currentTimeMillis();
	}

	public Value value() {
		return value;
	}

	public long lastAnnounced() {
		return lastAnnounced;
	}

	public void updateLastAnnounced() {
		this.lastAnnounced = System.currentTimeMillis();
	}
}
