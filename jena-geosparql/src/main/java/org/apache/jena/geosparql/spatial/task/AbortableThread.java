package org.apache.jena.geosparql.spatial.task;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbortableThread
    extends Thread
{
    private final AtomicBoolean requestingCancel;
    private volatile boolean cancelOnce = false;
    private Object cancelLock = new Object();

    private CompletableFuture<?> future = new CompletableFuture<>();

    public AbortableThread() {
        this(new AtomicBoolean());
    }

    public AbortableThread(AtomicBoolean requestingCancel) {
        super();
        this.requestingCancel = Objects.requireNonNull(requestingCancel);
    }

    public CompletableFuture<?> getFuture() {
        return future;
    }

    public final void run() {
        try {
            runInternal();
        } catch (Throwable t) {
            synchronized (cancelLock) {
                future.completeExceptionally(t);
            }
            throw new RuntimeException(t);
        } finally {
            synchronized (cancelLock) {
                future.complete(null);
            }
        }
    }

    public final void runInternal() throws Exception {
        try {
            runActual();
        } finally {
            doOnClose();
        }
    }

    public abstract void runActual() throws Exception;

    /** Returns true iff {@link #cancel()} was called. */
    public final boolean isCancelled() {
        return cancelOnce;
    }

    public final void cancel() {
        synchronized (cancelLock) {
            if ( ! cancelOnce && !future.isDone() ) {
                // Need to set the flags before allowing subclasses to handle requestCancel() in order
                // to prevent a race condition. We want to be sure that calls to have hasNext()/nextBinding()
                // will definitely throw a QueryCancelledException in this class and
                // not allow a situation in which a subclass component thinks it is cancelled,
                // while this class does not.
                if ( requestingCancel != null )
                    // Signalling from timeouts
                    requestingCancel.set(true);
                cancelOnce = true;
                this.requestCancel();
            }
        }
    }

    protected void requestCancel() {
        this.interrupt();
    }

    protected void doOnClose() {
    }
}
