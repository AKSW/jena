package org.apache.jena.geosparql.spatial.task;

import java.util.concurrent.Callable;

public interface Computation<T>
    extends Callable<T>
{
    void abort();
    void close();
}
