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

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Spatial Index Items in a Serializable form for file reading or writing.
 *
 */
public class SpatialIndexStorageGraph implements Serializable {

    private final String srsURI;
    private final List<StorageItemGraph> storageItems;

    public SpatialIndexStorageGraph(Collection<SpatialIndexItem> spatialIndexItems, String srsURI) {

        this.srsURI = srsURI;

        this.storageItems = new ArrayList<>(spatialIndexItems.size());

        for (SpatialIndexItem spatialIndexItem : spatialIndexItems) {
            String graph = null;
            if (spatialIndexItem instanceof SpatialIndexItemGraph) {
                graph = ((SpatialIndexItemGraph) spatialIndexItem).getGraphURI();
            }
            StorageItemGraph storageItem = new StorageItemGraph(spatialIndexItem.getEnvelope(), spatialIndexItem.getItem(), graph);
            storageItems.add(storageItem);
        }
    }

    public String getSrsURI() {
        return srsURI;
    }

    public Collection<SpatialIndexItem> getIndexItems() {

        List<SpatialIndexItem> indexItems = new ArrayList<>(storageItems.size());

        for (StorageItemGraph storageItem : storageItems) {
            SpatialIndexItem indexItem = storageItem.getIndexItem();
            indexItems.add(indexItem);
        }

        return indexItems;
    }

    public SpatialIndex getSpatialIndex() throws SpatialIndexException {
        return new SpatialIndex(getIndexItems(), srsURI);
    }

    private static class StorageItemGraph implements Serializable {

        private final Envelope envelope;
        private final String uri;
        private final String graphURI;

        public StorageItemGraph(Envelope envelope, Resource item) {
            this(envelope, item, null);
        }

        public StorageItemGraph(Envelope envelope, Resource item, String graphURI) {
            this.envelope = envelope;
            this.uri = item.getURI();
            this.graphURI = graphURI;
        }

        public Envelope getEnvelope() {
            return envelope;
        }

        public String getUri() {
            return uri;
        }

        public String getGraphURI() {
            return graphURI;
        }

        public Resource getItem() {
            return ResourceFactory.createResource(uri);
        }

        public SpatialIndexItem getIndexItem() {
            return (graphURI == null)
                    ? new SpatialIndexItem(envelope, getItem())
                    : new SpatialIndexItemGraph(envelope, getItem(), graphURI);
        }

        @Override
        public String toString() {
            return "StorageItem{" + "envelope=" + envelope + ", uri=" + uri + '}';
        }

    }
}
