package org.apache.jena.geosparql.spatial.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.jena.rdf.model.Resource;
import org.locationtech.jts.geom.*;

import java.io.Serializable;

public class GeometrySerde
        extends Serializer implements Serializable
{

    private static final GeometryFactory geometryFactory = new GeometryFactory();

    @Override
    public void write(Kryo kryo, Output out, Object object)
    {
       if (object instanceof Point || object instanceof LineString
                || object instanceof Polygon || object instanceof MultiPoint
                || object instanceof MultiLineString || object instanceof MultiPolygon) {
            writeType(out, Type.SHAPE);
            writeGeometry(kryo, out, (Geometry) object);
        }
        else if (object instanceof GeometryCollection) {
            GeometryCollection collection = (GeometryCollection) object;
            writeType(out, Type.GEOMETRYCOLLECTION);
            out.writeInt(collection.getNumGeometries());
            for (int i = 0; i < collection.getNumGeometries(); i++) {
                writeGeometry(kryo, out, collection.getGeometryN(i));
            }
            writeUserData(kryo, out, collection);
        }
        else if (object instanceof Envelope) {
            Envelope envelope = (Envelope) object;
            writeType(out, Type.ENVELOPE);
            out.writeDouble(envelope.getMinX());
            out.writeDouble(envelope.getMaxX());
            out.writeDouble(envelope.getMinY());
            out.writeDouble(envelope.getMaxY());
        } else if (object instanceof Resource) {
            writeType(out, Type.URI);
            kryo.writeObject(out, object);
        }
        else {
            throw new UnsupportedOperationException("Cannot serialize object of type " +
                    object.getClass().getName());
        }
    }

    private void writeType(Output out, Type type)
    {
        out.writeByte((byte) type.id);
    }

    private void writeGeometry(Kryo kryo, Output out, Geometry geometry)
    {
//        byte[] data = ShapeSerde.serialize(geometry);
//        out.write(data, 0, data.length);
//        writeUserData(kryo, out, geometry);
    }

    private void writeUserData(Kryo kryo, Output out, Geometry geometry)
    {
        out.writeBoolean(geometry.getUserData() != null);
        if (geometry.getUserData() != null) {
            kryo.writeClass(out, geometry.getUserData().getClass());
            kryo.writeObject(out, geometry.getUserData());
        }
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass)
    {
        byte typeId = input.readByte();
        Type geometryType = Type.fromId(typeId);
        switch (geometryType) {
            case SHAPE:
                return readGeometry(kryo, input);
            case GEOMETRYCOLLECTION: {
                int numGeometries = input.readInt();
                Geometry[] geometries = new Geometry[numGeometries];
                for (int i = 0; i < numGeometries; i++) {
                    geometries[i] = readGeometry(kryo, input);
                }
                GeometryCollection collection = geometryFactory.createGeometryCollection(geometries);
                collection.setUserData(readUserData(kryo, input));
                return collection;
            }
            case ENVELOPE: {
                double xMin = input.readDouble();
                double xMax = input.readDouble();
                double yMin = input.readDouble();
                double yMax = input.readDouble();
                return new Envelope(xMin, xMax, yMin, yMax);
            }
            case URI:
                return kryo.readObject(input, Resource.class);
            default:
                throw new UnsupportedOperationException(
                        "Cannot deserialize object of type " + geometryType);
        }
    }

    private Object readUserData(Kryo kryo, Input input)
    {
        Object userData = null;
        if (input.readBoolean()) {
            Registration clazz = kryo.readClass(input);
            userData = kryo.readObject(input, clazz.getType());
        }
        return userData;
    }

    private Geometry readGeometry(Kryo kryo, Input input)
    {
//        Geometry geometry = ShapeSerde.deserialize(input, geometryFactory);
//        geometry.setUserData(readUserData(kryo, input));
//        return geometry;
        return null;
    }

    private enum Type
    {
        SHAPE(0),
        CIRCLE(1),
        GEOMETRYCOLLECTION(2),
        ENVELOPE(3),
        URI(4);

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