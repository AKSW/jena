/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jena.geosparql.spatial;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.atlas.RuntimeIOException;
import org.apache.jena.atlas.io.IOX;
import org.apache.jena.geosparql.configuration.GeoSPARQLOperations;
import org.apache.jena.geosparql.implementation.GeometryWrapper;
import org.apache.jena.geosparql.implementation.SRSInfo;
import org.apache.jena.geosparql.implementation.registry.SRSRegistry;
import org.apache.jena.geosparql.implementation.vocabulary.Geo;
import org.apache.jena.geosparql.implementation.vocabulary.SRS_URI;
import org.apache.jena.geosparql.implementation.vocabulary.SpatialExtension;
import org.apache.jena.geosparql.spatial.serde.JtsKryoRegistrator;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.compose.Union;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.Symbol;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.STRtree;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.FactoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SpatialIndex for testing bounding box collisions between geometries within a
 * Dataset.<br>
 * Queries must be performed using the same SRS URI as the SpatialIndex.<br>
 * The SpatialIndex is added to the Dataset Context when it is built.<br>
 * QueryRewriteIndex is also stored in the SpatialIndex as its content is
 * Dataset specific.
 *
 */
public class SpatialIndex {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final Symbol SPATIAL_INDEX_SYMBOL = Symbol.create("http://jena.apache.org/spatial#index");
    public static final Symbol symSpatialIndexPerGraph = Symbol.create("http://jena.apache.org/spatial#indexPerGraph");
    public static final Symbol symSrsUri = Symbol.create("http://jena.apache.org/spatial#srsURI");

    private transient final SRSInfo srsInfo;
    private boolean isBuilt;
    private final STRtree defaultGraphTree;
    private Map<String, STRtree> graphToTree = new HashMap<>();
    private static final int MINIMUM_CAPACITY = 2;

    private File location;

    private SpatialIndex() {
        this.defaultGraphTree = new STRtree(MINIMUM_CAPACITY);
        this.isBuilt = true;
        this.defaultGraphTree.build();
        this.srsInfo = SRSRegistry.getSRSInfo(SRS_URI.DEFAULT_WKT_CRS84);
    }

    /**
     * Unbuilt Spatial Index with provided capacity.
     *
     * @param capacity
     * @param srsURI
     */
    public SpatialIndex(int capacity, String srsURI) {
        int indexCapacity = Math.max(capacity, MINIMUM_CAPACITY);
        this.defaultGraphTree = new STRtree(indexCapacity);
        this.isBuilt = false;
        this.srsInfo = SRSRegistry.getSRSInfo(srsURI);
    }

    /**
     * Built Spatial Index with provided capacity.
     *
     * @param spatialIndexItems
     * @param srsURI
     * @throws SpatialIndexException
     */
    public SpatialIndex(Collection<SpatialIndexItem> spatialIndexItems, String srsURI) throws SpatialIndexException {
        int indexCapacity = Math.max(spatialIndexItems.size(), MINIMUM_CAPACITY);
        this.defaultGraphTree = new STRtree(indexCapacity);
        insertItems(spatialIndexItems);
        this.defaultGraphTree.build();
        this.isBuilt = true;
        this.srsInfo = SRSRegistry.getSRSInfo(srsURI);
    }

    /**
     * Built Spatial Index with provided STR tree.
     *
     * @param tree
     * @param srsURI
     * @throws SpatialIndexException
     */
    public SpatialIndex(STRtree tree, String srsURI) throws SpatialIndexException {
        this.defaultGraphTree = tree;
        this.isBuilt = true;
        this.srsInfo = SRSRegistry.getSRSInfo(srsURI);
    }

    public SpatialIndex(STRtree defaultGraphTree, Map<String, STRtree> graphToTree, String srsURI) throws SpatialIndexException {
        this.defaultGraphTree = defaultGraphTree;
        this.graphToTree = graphToTree;
        this.isBuilt = true;
        this.srsInfo = SRSRegistry.getSRSInfo(srsURI);
    }

    /**
     *
     * @return Information about the SRS used by the SpatialIndex.
     */
    public SRSInfo getSrsInfo() {
        return srsInfo;
    }

    /**
     *
     * @return True if the SpatialIndex is empty.
     */
    public boolean isEmpty() {
        return defaultGraphTree.isEmpty();
    }

    /**
     *
     * @return True if the SpatialIndex has been built.
     */
    public boolean isBuilt() {
        return isBuilt;
    }

    /**
     * Build the Spatial Index. No more items can be added.
     */
    public void build() {
        if (!isBuilt) {
            graphToTree.values().forEach(STRtree::build);
            defaultGraphTree.build();
            isBuilt = true;
        }
    }

    /**
     * Returns the number of items in the index.
     */
    public int getSize() {
        return defaultGraphTree.size();
    }

    /**
     * Items to add to an unbuilt Spatial Index.
     *
     * @param indexItems
     * @throws SpatialIndexException
     */
    public final void insertItems(Collection<SpatialIndexItem> indexItems) throws SpatialIndexException {

        for (SpatialIndexItem indexItem : indexItems) {
            insertItem(indexItem.getEnvelope(), indexItem.getItem());
        }
    }

    /**
     * Item to add to an unbuilt Spatial Index.
     *
     * @param envelope
     * @param item
     * @throws SpatialIndexException
     */
    public final void insertItem(Envelope envelope, Resource item) throws SpatialIndexException {
        if (!isBuilt) {
            defaultGraphTree.insert(envelope, item.asNode());
        } else {
            throw new SpatialIndexException("SpatialIndex has been built and cannot have additional items.");
        }
    }

    @SuppressWarnings("unchecked")
    public HashSet<Node> query(Envelope searchEnvelope) {
        if (!defaultGraphTree.isEmpty()) {
            return new HashSet<>(defaultGraphTree.query(searchEnvelope));
        } else {
            return new HashSet<>();
        }
    }

    public HashSet<Node> query(Envelope searchEnvelope, String graph) {
        LOGGER.debug("spatial index lookup on graph: " + graph);

        // handle union graph
        if (graph.equals(Quad.unionGraph.getURI())) {
            LOGGER.warn("spatial index lookup on union graph");
            HashSet<Node> items = graphToTree.values().stream()
                    .map(tree -> tree.query(searchEnvelope))
                    .collect(HashSet::new,
                            Set::addAll,
                            Set::addAll);
            return items;
        } else {
            if (!graphToTree.containsKey(graph)) {
                LOGGER.warn("graph not indexed: " + graph);
            }
            STRtree tree = graphToTree.get(graph);
            if (tree != null && !tree.isEmpty()) {
                return new HashSet<>(tree.query(searchEnvelope));
            } else {
                return new HashSet<>();
            }
        }
    }

    public File getLocation() {
        return location;
    }

    public void setLocation(File location) {
        this.location = location;
    }

    public STRtree getDefaultGraphIndexTree() {
        return defaultGraphTree;
    }

    public Map<String, STRtree> getNamedGraphToIndexTreeMapping() {
        return graphToTree;
    }

    public boolean hasNamedGraphIndexed(String graph) {
        return graphToTree.containsKey(graph);
    }

    public boolean removeGraphFromIndex(String graph) {
        return graphToTree.remove(graph) != null;
    }

    @Override
    public String toString() {
        return "SpatialIndex{" + "srsInfo=" + srsInfo + ", isBuilt=" + isBuilt + ", strTree=" + defaultGraphTree + '}';
    }

    /**
     * Retrieve the SpatialIndex from the Context.
     *
     * @param execCxt
     * @return SpatialIndex contained in the Context.
     * @throws SpatialIndexException
     */
    public static final SpatialIndex retrieve(ExecutionContext execCxt) throws SpatialIndexException {

        Context context = execCxt.getContext();
        SpatialIndex spatialIndex = (SpatialIndex) context.get(SPATIAL_INDEX_SYMBOL, null);

        if (spatialIndex == null) {
            throw new SpatialIndexException("Dataset Context does not contain SpatialIndex.");
        }

        return spatialIndex;
    }

    /**
     *
     * @param execCxt
     * @return True if a SpatialIndex is defined in the ExecutionContext.
     */
    public static final boolean isDefined(ExecutionContext execCxt) {
        Context context = execCxt.getContext();
        return context.isDefined(SPATIAL_INDEX_SYMBOL);
    }

    /**
     * Set the SpatialIndex into the Context of the Dataset for later retrieval
     * and use in spatial functions.
     *
     * @param dataset
     * @param spatialIndex
     */
    public static final void setSpatialIndex(Dataset dataset, SpatialIndex spatialIndex) {
        Context context = dataset.getContext();
        context.set(SPATIAL_INDEX_SYMBOL, spatialIndex);
    }

    public static STRtree buildSpatialIndexTree(Model m, String srsURI) throws SpatialIndexException {
        Collection<SpatialIndexItem> items = getSpatialIndexItems(m, srsURI);
        STRtree tree = buildSpatialIndexTree(items);
        return tree;
    }

    public static STRtree buildSpatialIndexTree(Collection<SpatialIndexItem> items) throws SpatialIndexException {
        STRtree tree = new STRtree(Math.max(MINIMUM_CAPACITY, items.size()));
        items.forEach(item -> tree.insert(item.getEnvelope(), item.getItem().asNode()));
        LOGGER.info("{} geospatial features have been indexed", items.size());
        return tree;
    }

    /**
     * Build Spatial Index from all graphs in Dataset.<br>
     * Dataset contains SpatialIndex in Context.<br>
     * Spatial Index written to file.
     *
     * @param dataset
     * @param srsURI
     * @param spatialIndexFile
     * @return SpatialIndex constructed.
     * @throws SpatialIndexException
     */
    public static SpatialIndex buildSpatialIndex(Dataset dataset, String srsURI, File spatialIndexFile) throws SpatialIndexException {

        return buildSpatialIndex(dataset, srsURI, spatialIndexFile, false);
    }

    public static SpatialIndex buildSpatialIndex(Dataset dataset,
                                                 String srsURI,
                                                 File spatialIndexFile,
                                                 boolean spatialIndexPerGraph) throws SpatialIndexException {

        SpatialIndex spatialIndex = load(spatialIndexFile);

        if (spatialIndex.isEmpty()) {
            spatialIndex = buildSpatialIndex(dataset, srsURI, spatialIndexPerGraph);
            spatialIndex.build();

            save(spatialIndexFile, spatialIndex);
        }
        spatialIndex.setLocation(spatialIndexFile);

        setSpatialIndex(dataset, spatialIndex);
        return spatialIndex;
    }

    /**
     * Build Spatial Index from all graphs in Dataset.<br>
     * Dataset contains SpatialIndex in Context.<br>
     * SRS URI based on most frequent found in Dataset.<br>
     * Spatial Index written to file.
     *
     * @param dataset
     * @param spatialIndexFile
     * @return SpatialIndex constructed.
     * @throws SpatialIndexException
     */
    public static SpatialIndex buildSpatialIndex(Dataset dataset, File spatialIndexFile) throws SpatialIndexException {
        return buildSpatialIndex(dataset, spatialIndexFile, false);
    }

    public static SpatialIndex buildSpatialIndex(Dataset dataset, File spatialIndexFile, boolean spatialIndexPerGraph) throws SpatialIndexException {
        String srsURI = GeoSPARQLOperations.findModeSRS(dataset);
        SpatialIndex spatialIndex = buildSpatialIndex(dataset, srsURI, spatialIndexFile, spatialIndexPerGraph);
        return spatialIndex;
    }

    /**
     * Build Spatial Index from all graphs in Dataset.<br>
     * Dataset contains SpatialIndex in Context.
     *
     * @param dataset
     * @param srsURI
     * @return SpatialIndex constructed.
     * @throws SpatialIndexException
     */
    public static SpatialIndex buildSpatialIndex(Dataset dataset, String srsURI) throws SpatialIndexException {
        return buildSpatialIndex(dataset, srsURI, false);
    }

    public static SpatialIndex buildSpatialIndex(Dataset dataset, String srsURI, boolean indexTreePerGraph) throws SpatialIndexException {
        LOGGER.info("Building Spatial Index - Started");

        STRtree defaultIndexTree;
        Map<String, STRtree> graphToTree = new HashMap<>();

        // we always compute an index tree DGT for the default graph
        // if an index per named graph NG is enabled, we compute a separate index tree NGT for each NG, otherwise all
        // items will be indexed in the default graph index tree DGT

        if (indexTreePerGraph) {
            dataset.begin(ReadWrite.READ);
            LOGGER.info("building spatial index for default graph ...");
            Model defaultModel = dataset.getDefaultModel();
            defaultIndexTree = buildSpatialIndexTree(defaultModel, srsURI);

            // Named Models
            Iterator<String> graphNames = dataset.listNames();
            while (graphNames.hasNext()) {
                String graphName = graphNames.next();
                LOGGER.info("building spatial index for graph {} ...", graphName);
                Model namedModel = dataset.getNamedModel(graphName);
                graphToTree.put(graphName, buildSpatialIndexTree(namedModel, srsURI));
            }
            dataset.end();
        } else {
            LOGGER.info("building spatial index for default graph and all named graphs ...");
            Collection<SpatialIndexItem> spatialIndexItems = findSpatialIndexItems(dataset, srsURI);
            // create the union view of default graph and all named graphs
//            Union unionAllGraph = new Union(dataset.asDatasetGraph().getDefaultGraph(), dataset.asDatasetGraph().getUnionGraph());

            defaultIndexTree = buildSpatialIndexTree(spatialIndexItems);
        }

        LOGGER.info("Building Spatial Index - Completed");
        SpatialIndex index = new SpatialIndex(defaultIndexTree, graphToTree, srsURI);
        index.build();
        setSpatialIndex(dataset, index);
        return index;
    }

    /**
     * Find Spatial Index Items from all graphs in Dataset.<br>
     *
     * @param dataset
     * @param srsURI
     * @return SpatialIndexItems found.
     * @throws SpatialIndexException
     */
    public static Collection<SpatialIndexItem> findSpatialIndexItems(Dataset dataset, String srsURI) throws SpatialIndexException {
        //Default Model
        dataset.begin(ReadWrite.READ);
        Model defaultModel = dataset.getDefaultModel();
        Collection<SpatialIndexItem> items = getSpatialIndexItems(defaultModel, srsURI);
        LOGGER.info("found {} geospatial features in default graph", items.size());

        //Named Models
        Iterator<String> graphNames = dataset.listNames();
        while (graphNames.hasNext()) {
            String graphName = graphNames.next();
            Model namedModel = dataset.getNamedModel(graphName);
            Collection<SpatialIndexItem> graphItems = getSpatialIndexItems(namedModel, srsURI);
            LOGGER.info("found {} geospatial features in graph {}", graphItems.size(), graphName);
            items.addAll(graphItems);
        }

        dataset.end();

        return items;
    }


    /**
     * Build Spatial Index from all graphs in Dataset.<br>
     * Dataset contains SpatialIndex in Context.<br>
     * SRS URI based on most frequent found in Dataset.
     *
     * @param dataset
     * @return SpatialIndex constructed.
     * @throws SpatialIndexException
     */
    public static SpatialIndex buildSpatialIndex(Dataset dataset) throws SpatialIndexException {
        String srsURI = GeoSPARQLOperations.findModeSRS(dataset);
        SpatialIndex spatialIndex = buildSpatialIndex(dataset, srsURI);
        return spatialIndex;
    }

    /**
     * Recompute and replace the spatial index trees for the given named graphs.
     *
     * @param index   the spatial index to modify
     * @param dataset the dataset containing the named graphs
     * @param graphs  the named graphs
     * @return the modified spatial index object, i.e. no copy of the input index object
     * @throws SpatialIndexException
     */
    public static SpatialIndex recomputeIndexForGraphs(SpatialIndex index,
                                                       Dataset dataset,
                                                       List<String> graphs) throws SpatialIndexException {
        dataset.begin(ReadWrite.READ);
        for (String g : graphs) {
            if (index.graphToTree.containsKey(g)) {
                LOGGER.info("recomputing spatial index for graph: {}", g);
            } else {
                LOGGER.info("computing spatial index for graph: {}", g);
            }
            Model namedModel = dataset.getNamedModel(g);
            STRtree indexTree = buildSpatialIndexTree(namedModel, index.getSrsInfo().getSrsURI());
            STRtree oldIndexTree = index.graphToTree.put(g, indexTree);
            if (oldIndexTree != null) {
                LOGGER.info("replaced spatial index for graph: {}", g);
            } else {
                LOGGER.info("added spatial index for graph: {}", g);
            }
        }
        dataset.end();
        return index;
    }

    /**
     * Wrap Model in a Dataset and build SpatialIndex.
     *
     * @param model
     * @param srsURI
     * @return Dataset with default Model and SpatialIndex in Context.
     * @throws SpatialIndexException
     */
    public static final Dataset wrapModel(Model model, String srsURI) throws SpatialIndexException {

        Dataset dataset = DatasetFactory.createTxnMem();
        dataset.setDefaultModel(model);
        buildSpatialIndex(dataset, srsURI);

        return dataset;
    }

    /**
     * Wrap Model in a Dataset and build SpatialIndex.
     *
     * @param model
     * @return Dataset with default Model and SpatialIndex in Context.
     * @throws SpatialIndexException
     */
    public static final Dataset wrapModel(Model model) throws SpatialIndexException {
        Dataset dataset = DatasetFactory.createTxnMem();
        dataset.setDefaultModel(model);
        String srsURI = GeoSPARQLOperations.findModeSRS(dataset);
        buildSpatialIndex(dataset, srsURI);

        return dataset;
    }

    /**
     * Find items from the Model transformed to the SRS URI.
     *
     * @param model
     * @param srsURI
     * @return Items found in the Model in the SRS URI.
     * @throws SpatialIndexException
     */
    public static final Collection<SpatialIndexItem> getSpatialIndexItems(Model model, String srsURI) throws SpatialIndexException {

        List<SpatialIndexItem> items = new ArrayList<>();

        //Only add one set of statements as a converted dataset will duplicate the same info.
        if (model.contains(null, Geo.HAS_GEOMETRY_PROP, (Resource) null)) {
                LOGGER.info("Feature-hasGeometry-Geometry statements found.");
            if (model.contains(null, SpatialExtension.GEO_LAT_PROP, (Literal) null)) {
                LOGGER.warn("Lat/Lon Geo predicates also found but will not be added to index.");
            }
            Collection<SpatialIndexItem> geometryLiteralItems = getGeometryLiteralIndexItems(model, srsURI);
            items.addAll(geometryLiteralItems);
        } else if (model.contains(null, SpatialExtension.GEO_LAT_PROP, (Literal) null)) {
            LOGGER.info("Geo predicate statements found.");
            Collection<SpatialIndexItem> geoPredicateItems = getGeoPredicateIndexItems(model, srsURI);
            items.addAll(geoPredicateItems);
        }

        return items;
    }

    /**
     *
     * @param model
     * @param srsURI
     * @return GeometryLiteral items prepared for adding to SpatialIndex.
     * @throws SpatialIndexException
     */
    private static Collection<SpatialIndexItem> getGeometryLiteralIndexItems(Model model, String srsURI) throws SpatialIndexException {
        List<SpatialIndexItem> items = new ArrayList<>();
        StmtIterator stmtIt = model.listStatements(null, Geo.HAS_GEOMETRY_PROP, (Resource) null);
        while (stmtIt.hasNext()) {
            Statement stmt = stmtIt.nextStatement();

            Resource feature = stmt.getSubject();
            Resource geometry = stmt.getResource();

            ExtendedIterator<RDFNode> nodeIter = model.listObjectsOfProperty(geometry, Geo.HAS_SERIALIZATION_PROP);
            if (!nodeIter.hasNext()) {
                NodeIterator wktNodeIter = model.listObjectsOfProperty(geometry, Geo.AS_WKT_PROP);
                NodeIterator gmlNodeIter = model.listObjectsOfProperty(geometry, Geo.AS_GML_PROP);
                nodeIter = wktNodeIter.andThen(gmlNodeIter);
            }

            while (nodeIter.hasNext()) {
                Literal geometryLiteral = nodeIter.next().asLiteral();
                GeometryWrapper geometryWrapper = GeometryWrapper.extract(geometryLiteral);

                try {
                    //Ensure all entries in the target SRS URI.
                    GeometryWrapper transformedGeometryWrapper = geometryWrapper.convertSRS(srsURI);

                    Envelope envelope = transformedGeometryWrapper.getEnvelope();
                    SpatialIndexItem item = new SpatialIndexItem(envelope, feature);
                    items.add(item);
                } catch (FactoryException | MismatchedDimensionException | TransformException ex) {
                    throw new SpatialIndexException("Transformation Exception: " + geometryLiteral + ". " + ex.getMessage());
                }

            }
            nodeIter.close();
        }
        stmtIt.close();
        return items;
    }

    /**
     *
     * @param model
     * @param srsURI
     * @return Geo predicate objects prepared for adding to SpatialIndex.
     */
    private static Collection<SpatialIndexItem> getGeoPredicateIndexItems(Model model, String srsURI) throws SpatialIndexException {
        List<SpatialIndexItem> items = new ArrayList<>();
        ResIterator resIt = model.listResourcesWithProperty(SpatialExtension.GEO_LAT_PROP);

        while (resIt.hasNext()) {
            Resource feature = resIt.nextResource();

            Literal lat = feature.getRequiredProperty(SpatialExtension.GEO_LAT_PROP).getLiteral();
            Literal lon = feature.getProperty(SpatialExtension.GEO_LON_PROP).getLiteral();
            if (lon == null) {
                LOGGER.warn("Geo predicates: latitude found but not longitude. " + feature);
                continue;
            }

            Literal latLonPoint = ConvertLatLon.toLiteral(lat.getFloat(), lon.getFloat());
            GeometryWrapper geometryWrapper = GeometryWrapper.extract(latLonPoint);

            try {
                //Ensure all entries in the target SRS URI.
                GeometryWrapper transformedGeometryWrapper = geometryWrapper.convertSRS(srsURI);

                Envelope envelope = transformedGeometryWrapper.getEnvelope();
                SpatialIndexItem item = new SpatialIndexItem(envelope, feature);
                items.add(item);
            } catch (FactoryException | MismatchedDimensionException | TransformException ex) {
                throw new SpatialIndexException("Transformation Exception: " + geometryWrapper.getLexicalForm() + ". " + ex.getMessage());
            }
        }
        resIt.close();
        return items;
    }

    /**
     * Save SpatialIndex to file.
     *
     * @param spatialIndexFile the file being saved to
     * @param index the spatial index
     * @throws SpatialIndexException
     */
    public static final void save(File spatialIndexFile, SpatialIndex index) throws SpatialIndexException {

        if (spatialIndexFile != null) {
            LOGGER.info("Saving Spatial Index - Started: {}", spatialIndexFile.getAbsolutePath());

            String filename = spatialIndexFile.getAbsolutePath();
            Path file = Path.of(filename);
            Path tmpFile = IOX.uniqueDerivedPath(file, null);
            try {
                Files.deleteIfExists(file);
            } catch (IOException ex) {
                throw new SpatialIndexException("Failed to delete file: " + ex.getMessage());
            }
            try {
                Kryo kryo = new Kryo();
                JtsKryoRegistrator.registerClasses(kryo);

                IOX.safeWriteOrCopy(file, tmpFile,
                        out->{
                            Output output = new Output(out);
                            kryo.writeObject(output, index.srsInfo.getSrsURI());
                            kryo.writeObject(output, index.defaultGraphTree);
                            kryo.writeClassAndObject(output, index.graphToTree);
                            output.close();
                        });
            } catch (RuntimeIOException ex) {
                throw new SpatialIndexException("Save Exception: " + ex.getMessage());
            } finally {
                LOGGER.info("Saving Spatial Index - Completed: {}", spatialIndexFile.getAbsolutePath());
            }

        }
    }

    /**
     * Load a SpatialIndex from file.<br>
     * Index will be built and empty if file does not exist or is null.
     *
     * @param spatialIndexFile
     * @return Built Spatial Index.
     * @throws SpatialIndexException
     */
    public static final SpatialIndex load(File spatialIndexFile) throws SpatialIndexException {
        Kryo kryo = new Kryo();
        JtsKryoRegistrator.registerClasses(kryo);

        if (spatialIndexFile != null && spatialIndexFile.exists()) {
            LOGGER.info("Loading Spatial Index - Started: {}", spatialIndexFile.getAbsolutePath());

            try (Input input = new Input(new FileInputStream(spatialIndexFile))) {
                String srsUri = kryo.readObject(input, String.class);
                STRtree defaultGraphTree = kryo.readObject(input, STRtree.class);
                Map<String, STRtree> graphToTree = (Map<String, STRtree>) kryo.readClassAndObject(input);

                SpatialIndex spatialIndex = new SpatialIndex(defaultGraphTree, graphToTree, srsUri);
                spatialIndex.setLocation(spatialIndexFile);
                LOGGER.info("Loading Spatial Index - Completed: {}", spatialIndexFile.getAbsolutePath());
                return spatialIndex;
            } catch (IOException ex) {
                throw new SpatialIndexException("Loading Exception: " + ex.getMessage(), ex);
            }
        } else {
            LOGGER.info("File {} does not exist. Creating empty Spatial Index.", (spatialIndexFile != null ? spatialIndexFile.getAbsolutePath() : "null"));
            return new SpatialIndex();
        }
    }
}
