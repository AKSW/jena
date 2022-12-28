package org.apache.jena.geosparql.spatial.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.sedona.core.geometryObjects.GeometrySerde;
import org.apache.sedona.core.geometryObjects.SpatialIndexSerde;
import org.locationtech.jts.index.strtree.STRtree;

public class CustomSpatialIndexSerde extends SpatialIndexSerde {
    public CustomSpatialIndexSerde(GeometrySerde geometrySerde) {
        super(geometrySerde);
    }

    @Override
    public void write(Kryo kryo, Output output, Object o) {
        if (o instanceof STRtree) {
            //serialize rtree index
            output.writeByte((byte) 1);
            STRtree tree = (STRtree) o;
            org.locationtech.jts.index.strtree.IndexSerde indexSerde
                    = new org.locationtech.jts.index.strtree.IndexSerde();
            try {
                FieldUtils.writeField(indexSerde, "geometrySerde", new CustomGeometrySerde(), true);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            indexSerde.write(kryo, output, tree);
        } else {
            super.write(kryo, output, o);
        }
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        byte typeID = input.readByte();
        if (typeID == 1) {
            org.locationtech.jts.index.strtree.IndexSerde indexSerde =
                    new org.locationtech.jts.index.strtree.IndexSerde();
            try {
                FieldUtils.writeField(indexSerde, "geometrySerde", new CustomGeometrySerde(), true);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            return indexSerde.read(kryo, input);
        } else {
            return super.read(kryo, input, aClass);
        }

    }
}