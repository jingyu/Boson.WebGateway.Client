package io.bosonnetwork.higgs;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.vertx.core.Vertx;

public class VertxScheduledExecutorService implements ScheduledExecutorService {
	private final Vertx vertx;

	static class VertxScheduledFuture<T> implements ScheduledFuture<T>, Runnable {
		private static final int NEW = 0;
		private static final int COMPLETING = 1;
		private static final int NORMAL = 2;
		private static final int EXCEPTIONAL = 3;
		private static final int CANCELLED = 4;

		private volatile int state;

		private Vertx vertx;
		// set by the scheduler
		long timerId;

		private Callable<T> callable;
		private long period;
		private long nextTriggerTime;
		private Object outcome;

		private Object done = new Object();

		VertxScheduledFuture(Vertx vertx, Callable<T> callable, long delay, long period) {
			this.vertx = vertx;
			this.callable = callable;
			this.period = period;
			this.nextTriggerTime = System.currentTimeMillis() + delay;
			state = NEW;
		}

		VertxScheduledFuture(Vertx vertx, Runnable command, T result, long delay, long period) {
			this.vertx = vertx;
			this.callable = () -> {
				command.run();
				return result;
			};
			this.period = period;
			this.nextTriggerTime = System.currentTimeMillis() + delay;
			state = NEW;
		}

		private void setState(int state) {
			this.state = state;
			if (state > COMPLETING) {
				synchronized (done) {
					done.notifyAll();
				}
			}
		}

		private void await(long timeoutMillis, int nanos) throws InterruptedException {
			synchronized (done) {
				done.wait(timeoutMillis, nanos);
			}
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return unit.convert(nextTriggerTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
		}

		@Override
		public int compareTo(Delayed o) {
			return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			boolean cancelled = vertx.cancelTimer(timerId);
			if (cancelled)
				setState(CANCELLED);
			return cancelled;
		}

		@Override
		public boolean isCancelled() {
			return state >= CANCELLED;
		}

		@Override
		public boolean isDone() {
			return state > COMPLETING;
		}

		@SuppressWarnings("unchecked")
		private T populateOutcome() throws ExecutionException {
			int s = state;
			Object x = outcome;
			if (s == NORMAL)
				return (T) x;
			if (s >= CANCELLED)
				throw new CancellationException();

			throw new ExecutionException((Throwable) x);
		}

		@Override
		public T get() throws InterruptedException, ExecutionException {
			int s = state;
			if (s <= COMPLETING)
				await(0L, 0);

			return populateOutcome();
		}

		@Override
		public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			Objects.requireNonNull(unit, "unit");

			int s = state;
			if (s <= COMPLETING) {
				long timeoutMillis = unit.toMillis(timeout);
				int nanos = (int) (unit.toNanos(timeout) - unit.toNanos(timeoutMillis));
				await(timeoutMillis, nanos);
			}

			if (s <= COMPLETING)
				throw new TimeoutException();

			return populateOutcome();
		}

		@Override
		public void run() {
			if (period != 0)
				nextTriggerTime = System.currentTimeMillis() + period;

			try {
				setState(COMPLETING);
				outcome = callable.call();
				setState(NORMAL);
			} catch (Exception e) {
				outcome = e;
				setState(EXCEPTIONAL);
			}
		}
	}

	VertxScheduledExecutorService(Vertx vertx) {
		this.vertx = vertx;
	}

	@Override
	public void shutdown() {
		throw new UnsupportedOperationException();

	}

	@Override
	public List<Runnable> shutdownNow() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isShutdown() {
		return false;
	}

	@Override
	public boolean isTerminated() {
		return false;
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		Objects.requireNonNull(task, "task");

		return schedule(task, 0, TimeUnit.MILLISECONDS);
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		Objects.requireNonNull(task, "task");

		return schedule(task, result, 0, TimeUnit.MILLISECONDS);
	}

	@Override
	public Future<?> submit(Runnable task) {
		Objects.requireNonNull(task, "task");

		return schedule(task, 0, TimeUnit.MILLISECONDS);
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void execute(Runnable command) {
		Objects.requireNonNull(command, "command");

		vertx.executeBlocking(() -> {
			command.run();
			return null;
		});
	}

	@Override
	public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
		Objects.requireNonNull(command, "command");
		Objects.requireNonNull(unit, "unit");

		return schedule(command, null, delay, unit);
	}

	private <T> ScheduledFuture<T> schedule(Runnable command, T result, long delay, TimeUnit unit) {
		Objects.requireNonNull(command, "command");
		Objects.requireNonNull(unit, "unit");

		long delayMillis = unit.toMillis(delay);

		VertxScheduledFuture<T> future = new VertxScheduledFuture<>(vertx, command, result, delayMillis, 0);

		future.timerId = vertx.setTimer(delayMillis, id -> {
			if (!future.isCancelled()) {
				execute(future);
			}
		});

		return future;
	}

	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
		Objects.requireNonNull(callable, "callable");
		Objects.requireNonNull(unit, "unit");

		long delayMillis = unit.toMillis(delay);

		VertxScheduledFuture<V> future = new VertxScheduledFuture<>(vertx, callable, delayMillis, 0);

		future.timerId = vertx.setTimer(delayMillis, id -> {
			if (!future.isCancelled()) {
				execute(future);
			}
		});

		return future;
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
		Objects.requireNonNull(command, "command");
		Objects.requireNonNull(unit, "unit");

		long initialDelayMillis = unit.toMillis(initialDelay);
		long periodMillis = unit.toMillis(period);

		VertxScheduledFuture<Void> future = new VertxScheduledFuture<>(vertx, command,
				null, initialDelayMillis, periodMillis);
		future.timerId = vertx.setPeriodic(initialDelayMillis, periodMillis, (tid) -> {
			if (!future.isCancelled()) {
				execute(future);
			}
		});

		return future;
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
		Objects.requireNonNull(command, "command");
		Objects.requireNonNull(unit, "unit");

		long initialDelayMillis = unit.toMillis(initialDelay);
		long periodMillis = unit.toMillis(delay);

		// TODO: improve to fixed delay!!!
		VertxScheduledFuture<Void> future = new VertxScheduledFuture<>(vertx, command,
				null, initialDelayMillis, periodMillis);
		future.timerId = vertx.setPeriodic(initialDelayMillis, periodMillis, (tid) -> {
			if (!future.isCancelled()) {
				execute(future);
			}
		});

		return future;
	}
}
