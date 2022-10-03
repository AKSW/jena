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
import org.locationtech.jts.geom.Envelope;

/**
 *
 *
 */
public class SpatialIndexItemGraph extends SpatialIndexItem {

    private final String graphURI;

    public SpatialIndexItemGraph(Envelope envelope, Resource item) {
        this(envelope, item, null);
    }

    public SpatialIndexItemGraph(Envelope envelope, Resource item, String graphURI) {
        super(envelope, item);
        this.graphURI = graphURI;
    }

    public SpatialIndexItemGraph(SpatialIndexItem item, String graphURI) {
        super(item.getEnvelope(), item.getItem());
        this.graphURI = graphURI;
    }

    public String getGraphURI() {
        return graphURI;
    }

    @Override
    public String toString() {
        return "SpatialIndexItem{" + "envelope=" + getEnvelope() + ", item=" + getItem() + '}';
    }

}
