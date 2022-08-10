package org.apache.jena.geosparql.spatial.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.Serializable;

public class SpatialIndexSerde
        extends Serializer implements Serializable
{

    private final GeometrySerde geometrySerde;

    public SpatialIndexSerde()
    {
        super();
        geometrySerde = new GeometrySerde();
    }

    public SpatialIndexSerde(GeometrySerde geometrySerde)
    {
        super();
        this.geometrySerde = geometrySerde;
    }

    @Override
    public void write(Kryo kryo, Output output, Object o) {
        if (o instanceof Quadtree) {
            // serialize quadtree index
            writeType(output, Type.QUADTREE);
            Quadtree tree = (Quadtree) o;
            org.locationtech.jts.index.quadtree.IndexSerde indexSerde = new org.locationtech.jts.index.quadtree.IndexSerde();
            indexSerde.write(kryo, output, tree);
        } else if (o instanceof STRtree) {
            //serialize rtree index
            writeType(output, Type.RTREE);
            STRtree tree = (STRtree) o;
            org.locationtech.jts.index.strtree.IndexSerde indexSerde = new org.locationtech.jts.index.strtree.IndexSerde();
            indexSerde.write(kryo, output, tree);
        } else {
            throw new UnsupportedOperationException(" index type not supported ");
        }
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        byte typeID = input.readByte();
        Type indexType = Type.fromId(typeID);
        switch (indexType) {
            case QUADTREE: {
                org.locationtech.jts.index.quadtree.IndexSerde indexSerde =
                        new org.locationtech.jts.index.quadtree.IndexSerde();
                return indexSerde.read(kryo, input);
            }
            case RTREE: {
                org.locationtech.jts.index.strtree.IndexSerde indexSerde =
                        new org.locationtech.jts.index.strtree.IndexSerde();
                return indexSerde.read(kryo, input);
            }
            default: {
                throw new UnsupportedOperationException("can't deserialize spatial index of type" + indexType);
            }
        }
    }

    private void writeType(Output output, Type type)
    {
        output.writeByte((byte) type.id);
    }

    private enum Type
    {

        QUADTREE(0),
        RTREE(1);

        private final int id;

        Type(int id)
        {
            this.id = id;
        }

        public static Type fromId(int id)
        {
            for (Type type : values()) {
                if (type.id == id) {
                    return type;
                }
            }

            return null;
        }
    }
}