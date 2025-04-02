package org.apache.jena.geosparql.spatial.task;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskControlBase<S>
    implements TaskControl<S>
{
    private static final Logger logger = LoggerFactory.getLogger(TaskControlBase.class);

    /** A source object for this task. */
    protected S source;
    protected String label;

    protected List<Consumer<Throwable>> completionHandlers = new ArrayList<>();
    protected Runnable abortAction;

    protected Throwable throwable;
    protected boolean isComplete;

    protected volatile boolean hasBeenAborted = false;

    public TaskControlBase(String label) {
        super();
        this.label = label;
        // this.setAbortAction(abortAction);
    }

    public void setSource(S source) {
        this.source = source;
    }

    @Override
    public S getSource() {
        return source;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public synchronized void abort() {
        hasBeenAborted = true;

        // The task may have failed before the abort action became available
        if (abortAction != null) {
            abortAction.run();
        }
    }

    @Override
    public boolean isAborting() {
        return hasBeenAborted;
    }

    @Override
    public boolean isComplete() {
        return isComplete;
    }

    @Override
    public Throwable getThrowable() {
        return throwable;
    }

    @Override
    public Registration whenComplete(Consumer<Throwable> handler) {
        // If completed then trigger immediately, otherwise enqueue until done.
        if (isComplete) {
            handler.accept(throwable);
        } else {
            completionHandlers.add(handler);
        }
        return () -> completionHandlers.remove(handler);
    }

    public synchronized void setAbortAction(Runnable action) {
        this.abortAction = action;

        if (hasBeenAborted) {
            abort();
        }
    }

    void complete(Throwable throwable) {
        if (isComplete()) {
            throw new IllegalStateException("Must not complete more than once.");
        }

        this.throwable = throwable;
        this.isComplete = true;

        fireEvents();
    }

    protected void fireEvents() {
        for (Consumer<Throwable> handler : completionHandlers) {
            try {
                handler.accept(throwable);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("A task completion handler raised an exception.", t);
                }
            }
        }
        completionHandlers.clear();
    }
}
