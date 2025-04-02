package org.apache.jena.geosparql.spatial.task;

import java.util.function.Consumer;

//public class TaskWrapper<S>
//    implements TaskControl<S>
//{
//    protected TaskControl<S> delegate;
//    protected Registration completionListener;
//
//    public TaskControl<S> getDelegate() {
//        return delegate;
//    }
//
//    @Override
//    public String getLabel() {
//        return getDelegate().getLabel();
//    }
//
//    @Override
//    public S getSource() {
//        return getDelegate().getSource();
//    }
//
//    @Override
//    public void abort() {
//        getDelegate().abort();
//    }
//
//    @Override
//    public boolean isComplete() {
//        return getDelegate().isComplete();
//    }
//
//    @Override
//    public Throwable getThrowable() {
//        return getDelegate().getThrowable();
//    }
//
//    @Override
//    public Registration whenComplete(Consumer<Throwable> action) {
//        throw new UnsupportedOperationException();
//        // return null;
//    }
//
////    public boolean detach() {
////
////    }
//}
