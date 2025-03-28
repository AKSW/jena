package org.apache.jena.geosparql.spatial.index.v2;

import java.lang.invoke.MethodHandles;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jena.geosparql.spatial.SpatialIndexException;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.system.AutoTxn;
import org.apache.jena.system.Txn;
import org.locationtech.jts.index.strtree.STRtree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpatialIndexerTask
    // extends TaskControlBase<Object>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * The list of physical graphs to process.
     * May use the default graph name but not e.g. union default graph
     */
    private DatasetGraph datasetGraph;
    private List<Node> graphNodes;
    private ExecutorService executorService;
    private String srsURI;

    private final AtomicBoolean requestingCancel = new AtomicBoolean();
    private volatile boolean cancelOnce = false;
    private Object cancelLock = new Object();

    // private CompletionService<Entry<Node, STRtree>> completionService;
    private List<Future<Entry<Node, STRtree>>> futures = null;

    // private boolean parallel;
    // private Stream<Node> graphNodeStream;

    public SpatialIndexerTask(DatasetGraph datasetGraph, String srsURI, List<Node> graphNodes, boolean parallel) {
        // super(source, label);
        this.datasetGraph = datasetGraph;
        this.graphNodes = graphNodes;
        this.srsURI = srsURI;
    }

    private Entry<Node, STRtree> subTask(Node graphNode) throws SpatialIndexException {
        LOGGER.info("building spatial index for graph {} ...", graphNode);
        STRtree tree;

        try (AutoTxn txn = Txn.begin(datasetGraph, TxnType.READ)) {
            Graph graph = Quad.isDefaultGraph(graphNode) // XXX would getGraph work with the default graph URIs?
                ? datasetGraph.getDefaultGraph()
                : datasetGraph.getGraph(graphNode);
            tree = STRtreeUtils.buildSpatialIndexTree(graph, srsURI);
        }

        return Map.entry(graphNode, tree);
    }

    public SpatialIndexPerGraph call() throws InterruptedException, ExecutionException {
        synchronized (cancelLock) {
            if (executorService != null) {
                throw new IllegalStateException("Task already running.");
            }

            executorService = Executors.newCachedThreadPool();
        }

        // Beware: We expect special graphNodes such as Quad.unionGraph to have been resolved before coming here.
        for (Node graphNode : graphNodes) {
            checkRequestingCancel();
            Future<Entry<Node, STRtree>> future = executorService.submit(() -> subTask(graphNode));
            futures.add(future);
        }

        STRtree defaultTree = null;
        Map<Node, STRtree> namedTrees = new LinkedHashMap<Node, STRtree>();

        for (Future<Entry<Node, STRtree>> future : futures) {
            Entry<Node, STRtree> entry = future.get();
            Node graphNode = entry.getKey();
            STRtree tree = entry.getValue();

            if (Quad.isDefaultGraph(graphNode)) {
                if (defaultTree != null) {
                    // Preprocessing should avoid this case.
                    LOGGER.warn("Discarding duplicate index for default graph.");
                } else {
                    defaultTree = tree;
                }
            } else {
                namedTrees.put(graphNode, defaultTree);
            }
        }

        STRtreePerGraph trees = new STRtreePerGraph(defaultTree, namedTrees);
        SpatialIndexPerGraph result = new SpatialIndexPerGraph(trees);
        return result;
    }

    protected void checkRequestingCancel() {
        boolean isCancelled = requestingCancel();
        if (isCancelled) {
            throw new CancellationException();
        }
    }

    public void abort() {
        synchronized (cancelLock) {
            requestingCancel.set(true);
            if (!cancelOnce) {
                requestCancel();
                cancelOnce = true;
            }
        }
    }

    public void close() {
        // if (futures != null) {
        //     futures.forEach(future -> future.cancel(true));
        // }

        executorService.shutdownNow();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException("Abandoning an executor serivce that failed to stop.", e);
        }
    }

    protected void requestCancel() { }

    private boolean requestingCancel() {
        return (requestingCancel != null && requestingCancel.get()) || Thread.interrupted() ;
    }

    protected void performRequestingCancel() {
        // Tasks will be aborted on close - so nothing to do here.
    }
}
