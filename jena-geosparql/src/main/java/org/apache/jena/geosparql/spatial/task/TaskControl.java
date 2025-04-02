package org.apache.jena.geosparql.spatial.task;

import java.util.function.Consumer;

/** An outside view of a running task */
public interface TaskControl<S> {
        @FunctionalInterface
        interface Registration {
            void dispose();
        }

        String getLabel();
        S getSource();
        void abort();
        boolean isAborting();

        boolean isComplete();
        Throwable getThrowable(); // Only meaningful if isComplete is true.

        // CompletableFuture<?> getFuture();
        /** Retry a failed task. May only be called when isComplete() returns true */
        // void retry();

        /**
         * Registered actions are run only once, then the registration is removed automatically.
         *
         * @param action The action is invoked with any thrown exception - null if there was none.
         * @return A registration that can be used to unregister the listener early.
         */
        Registration whenComplete(Consumer<Throwable> action);
}
