package org.apache.jena.geosparql.spatial.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import org.apache.jena.geosparql.spatial.task.TaskControl.Registration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread base class that provides {@link #cancel()} and {@link #requestingCancel} methods
 * that can run a custom action (in addition to setting the interrupted flag)
 * as well as a {@link #doAfterRun()} method for cleaning up after execution
 * (only called if there is a prior call to run).
 */
public abstract class AbortableThread<T>
    extends Thread
{
    private static final Logger logger = LoggerFactory.getLogger(AbortableThread.class);

    private final AtomicBoolean requestingCancel;
    private volatile boolean cancelOnce = false;
    private Object cancelLock = new Object();

    protected List<BiConsumer<? super T, Throwable>> completionHandlers = new ArrayList<>();

    private boolean isComplete = false;
    private T value = null;
    private Throwable throwable;

    // XXX Natively support a completable future?
    // private CompletableFuture<?> future = new CompletableFuture<>();

    public AbortableThread() {
        this(new AtomicBoolean());
    }

    public AbortableThread(AtomicBoolean requestingCancel) {
        super();
        this.requestingCancel = Objects.requireNonNull(requestingCancel);
    }

//    public CompletableFuture<?> getFuture() {
//        return future;
//    }

    public final boolean isComplete() {
        return isComplete;
    }

    public T getValue() {
        return value;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public final void run() {
        try {
            runInternal();
        } finally {
            synchronized (cancelLock) {
                fireEvents();
                // future.complete(null);
            }
        }
    }

    public final void runInternal() {
        try {
            runActual();
        } catch (Throwable t) {
            // t.addSuppressed(new RuntimeException("An error occurred."));
            throwable = t;
            throw new RuntimeException(t);
            // throw t;
        } finally {
            isComplete = true;
            doAfterRun();
        }
    }

    public abstract void runActual() throws Exception;

    /** Returns true iff {@link #cancel()} was called. */
    public final boolean isCancelled() {
        return cancelOnce;
    }

    public final void cancel() {
        synchronized (cancelLock) {
            if ( ! cancelOnce && !isComplete ) {
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

    protected void doAfterRun() {
    }

    protected void fireEvents() {
        for (BiConsumer<? super T, Throwable> handler : completionHandlers) {
            try {
                handler.accept(null, throwable);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("A task completion handler raised an exception.", t);
                }
            }
        }
    }

    /**
     * Registered actions are run only once, then the registration is removed automatically.
     *
     * @param action The action is invoked with any thrown exception - null if there was none.
     * @return A registration that can be used to unregister the listener early.
     */
    protected Registration whenComplete(BiConsumer<? super T, Throwable> action) {
        Objects.requireNonNull(action);
        synchronized (cancelLock) {
            boolean isAdded = completionHandlers.add(action);
            if (isAdded) {
                // Immediately resolve if already complete.
                if (isComplete()) {
                    action.accept(value, throwable);
                }
            }
            return () -> {
                synchronized (cancelLock) {
                    completionHandlers.remove(action);
                }
            };
        }
    }
}
