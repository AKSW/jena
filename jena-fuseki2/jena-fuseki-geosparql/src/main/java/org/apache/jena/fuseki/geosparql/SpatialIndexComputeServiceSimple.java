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

package org.apache.jena.fuseki.geosparql;

import org.apache.jena.atlas.lib.DateTimeUtils;
import org.apache.jena.fuseki.FusekiException;
import org.apache.jena.fuseki.servlets.BaseActionREST;
import org.apache.jena.fuseki.servlets.GraphTarget;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.ServletOps;
import org.apache.jena.geosparql.spatial.SpatialIndex;
import org.apache.jena.geosparql.spatial.SpatialIndexException;
import org.apache.jena.graph.Graph;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.WebContent;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.web.HttpSC;

import java.io.File;
import java.io.IOException;

import static java.lang.String.format;
import static org.apache.jena.fuseki.servlets.GraphTarget.determineTarget;

/**
 * Spatial index (re)computation service.
 */
public class SpatialIndexComputeServiceSimple extends BaseActionREST { //ActionREST {

    public SpatialIndexComputeServiceSimple() {
    }

    @Override
    protected void doPost(HttpAction action) {

        String spatialIndexFilePathStr = action.getRequestParameter("spatial-index-file");
        boolean inMemory = spatialIndexFilePathStr == null;

        DatasetGraph dsg = action.getDataset();

        action.beginRead();
        GraphTarget graphTarget = determineTarget(dsg, action);
        if (!graphTarget.exists())
            ServletOps.errorNotFound("No data graph: " + graphTarget.label());
        Graph data = graphTarget.graph();
        action.end();

        Dataset ds = DatasetFactory.wrap(dsg);
        try {
            action.log.info(format("[%d] spatial index: computation started", action.id));
            if (inMemory) {
                SpatialIndex.buildSpatialIndex(ds, false);
            } else {
                SpatialIndex.buildSpatialIndex(ds, new File(spatialIndexFilePathStr), false);
            }

        } catch (SpatialIndexException e) {
            throw new RuntimeException(e);
        }

        action.log.info(format("[%d] spatial index: computation finished", action.id));
        action.setResponseStatus(HttpSC.OK_200);
        action.setResponseContentType(WebContent.contentTypeTextPlain);
        try {
            action.getResponseOutputStream().print("Spatial index computation completed at " + DateTimeUtils.nowAsXSDDateTimeString());
        } catch (IOException e) {
            throw new FusekiException(e);
        }
    }
}
