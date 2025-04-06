package org.apache.jena.geosparql.spatial.task;

import java.util.Objects;
import java.util.function.Consumer;

public class TaskControlOverAbortableThread<S>
    implements TaskControl<S>
{
    protected String label;
    protected S source;
    protected AbortableThread<?> thread;

    public TaskControlOverAbortableThread(String label) {
        super();
        this.label = label;
    }

    public void setThread(AbortableThread<?> thread) {
        this.thread = thread;
    }

    public void setSource(S source) {
        this.source = source;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public S getSource() {
        return source;
    }

    protected AbortableThread<?> requireThread() {
        Objects.requireNonNull(thread);
        return thread;
    }

    @Override
    public void abort() {
        requireThread().cancel();
    }

    @Override
    public boolean isAborting() {
        return requireThread().isCancelled();
    }

    @Override
    public boolean isComplete() {
        return !requireThread().isAlive();
    }

    @Override
    public Throwable getThrowable() {
        return requireThread().getThrowable();
    }

    @Override
    public Registration whenComplete(Consumer<Throwable> action) {
        // Note: The thread's result value is discarded here.
        return requireThread().whenComplete((v, t) -> action.accept(t));
    }

}
