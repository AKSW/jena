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

package org.apache.jena.riot.system;

import org.apache.jena.riot.protobuf.ProtobufConvert;
import org.apache.jena.riot.protobuf.wire.PB_RDF;
import org.apache.jena.sparql.core.Quad;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

/** Serialization of a {@link Quad} using Thrift for the serialization. */
public final class SQuadProtobuf implements Serializable {
    private static final long serialVersionUID = 8043353997830878307L;
    private transient Quad quad;

    public SQuadProtobuf(Quad quad)     { this.quad = quad; }
    public Quad getQuad()       { return quad; }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        PB_RDF.RDF_Quad rdfQuad = ProtobufConvert.convert(quad, false);
        rdfQuad.writeTo(out);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException {
        PB_RDF.RDF_Quad rdfQuad = PB_RDF.RDF_Quad.parseFrom(in);
        quad = ProtobufConvert.convert(rdfQuad);
    }

    Object readResolve() throws ObjectStreamException
    { return quad; }
}
