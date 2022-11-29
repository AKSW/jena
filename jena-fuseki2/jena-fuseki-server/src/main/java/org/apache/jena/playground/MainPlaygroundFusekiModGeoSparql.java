package org.apache.jena.playground;

import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.sys.FusekiModules;
import org.apache.jena.fuseki.mod.geosparql.FMod_SpatialIndexer;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.query.DatasetFactory;

public class MainPlaygroundFusekiModGeoSparql {
    public static void main(String[] args) {
        FusekiLogging.setLogging();

        FMod_SpatialIndexer myModule = new FMod_SpatialIndexer();
        myModule.start();
        FusekiModules fmods = FusekiModules.create(myModule);
        FusekiServer server = FusekiServer.create()
            .fusekiModules(fmods)
            .staticFileBase("/home/raven/Laboratory/webapp")
            .add("test", DatasetFactory.create())
            .port(3030)
            .enablePing(true)
            .enableStats(true)
            .enableTasks(true)
            .build();

        server.start();
    }
}
