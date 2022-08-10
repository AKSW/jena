package org.apache.jena.geosparql.spatial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.geosparql.spatial.serde.GeometrySerde;
import org.apache.jena.geosparql.spatial.serde.SpatialIndexSerde;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JenaGeoSPARQLKryoRegistrator {
    final static Logger log = LoggerFactory.getLogger(JenaGeoSPARQLKryoRegistrator.class);

    public void registerClasses(Kryo kryo) {
        GeometrySerde serializer = new GeometrySerde();
        SpatialIndexSerde indexSerializer = new SpatialIndexSerde(serializer);

        log.info("Registering custom serializers for geometry types");

        kryo.register(Point.class, serializer);
        kryo.register(LineString.class, serializer);
        kryo.register(Polygon.class, serializer);
        kryo.register(MultiPoint.class, serializer);
        kryo.register(MultiLineString.class, serializer);
        kryo.register(MultiPolygon.class, serializer);
        kryo.register(GeometryCollection.class, serializer);
        kryo.register(Envelope.class, serializer);
        // TODO: Replace the default serializer with default spatial index serializer
        kryo.register(Quadtree.class, indexSerializer);
        kryo.register(STRtree.class, indexSerializer);
        kryo.register(Resource.class, new ResourceSerializer());
        kryo.register(ResourceImpl.class, new ResourceSerializer());
    }

    static class ResourceSerializer extends Serializer<Resource> {

        @Override
        public void write(Kryo kryo, Output output, Resource resource) {
            output.writeString(resource.getURI());
        }

        @Override
        public Resource read(Kryo kryo, Input input, Class<Resource> aClass) {
            return ResourceFactory.createResource(input.readString());
        }
    }
}