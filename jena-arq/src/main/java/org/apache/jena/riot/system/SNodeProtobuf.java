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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.riot.protobuf.ProtobufConvert;
import org.apache.jena.riot.protobuf.wire.PB_RDF;

import java.io.*;

/** Serialization of a {@link Node} using Thrift for the serialization. */  
public final class SNodeProtobuf implements Serializable {
    private static final long serialVersionUID = 5312954454377250166L;
    private transient Node node;
    private static PB_RDF.RDF_Term.Builder term_builder = PB_RDF.RDF_Term.newBuilder();

    public SNodeProtobuf(Node node) { this.node = node; }
    public Node getNode()   { return node; }
    
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        PB_RDF.RDF_Term rdfTerm = ProtobufConvert.toProtobuf(node, term_builder);
        rdfTerm.writeTo(out);
    }
    
    private void readObject(java.io.ObjectInputStream in) throws IOException {
        PB_RDF.RDF_Term rdfTerm = PB_RDF.RDF_Term.parseFrom(in);
        node = ProtobufConvert.convert(rdfTerm);
    }
    
    Object readResolve() throws ObjectStreamException
    { return node; }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        org.apache.jena.sys.Serializer.setNodeSerializer(SNodeProtobuf::new);
        Node blabb = NodeFactory.createBlankNode("blabb");
        Node xxx = NodeFactory.createBlankNode();
        Node lit = NodeFactory.createLiteral("lit");
        Node ex = NodeFactory.createURI("http://www.example.org/x");
        ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream("/tmp/oos"));
        os.writeObject(blabb);
        os.writeObject(xxx);
        os.writeObject(lit);
        os.writeObject(ex);
        os.close();

        ObjectInputStream is = new ObjectInputStream(new FileInputStream("/tmp/oos"));
        for (int i = 0; i < 4; i += 1) {
            Object o = is.readObject();
            System.out.println(o.getClass());
            System.out.println(o);
        }
    }
}
