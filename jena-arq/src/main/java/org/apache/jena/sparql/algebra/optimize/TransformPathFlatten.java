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

package org.apache.jena.sparql.algebra.optimize;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpPath;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.core.PathBlock;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.path.*;

/**
 * Perform the equivalent of the transactions in the SPARQL 1.1 spec. ARQ
 * regards path transformation as an optimization. ARQ does not execute the
 * exact transformation as per spec as there are better ways to do it for ARQ.
 * For example,
 * <ul>
 * <li>Path seq {@literal ->} BGPs or a (sequence)
 * <li>"|" is expanded into a union.
 * </ul>
 */

public class TransformPathFlatten extends TransformCopy
{
    // This transform is also used so programmatically built queries also get converted.
    // Need previous BGP for merging?  Do as a separate pass (sequence, BGP collapse)

    private PathCompiler pathCompiler ;

    public TransformPathFlatten() { this(new PathCompiler()) ; }

    public TransformPathFlatten(PathCompiler pathCompiler)
    {
        this.pathCompiler = pathCompiler ;
    }

    @Override
    public Op transform(OpPath opPath)
    {
        Op op = transformReduce(opPath, pathCompiler);
        return op;
    }

    static Op transformReduce(OpPath opPath, PathCompiler pathCompiler) {
        // Flatten down to triples where possible.
        PathBlock pattern = pathCompiler.reduce(opPath.getTriplePath());
        // Any generated paths of exactly one to triple; convert to Op.
        Op op = PathLib.pathToTriples(pattern);
        return transformAlt(op, pathCompiler);
    }

    static Op transformAlt(Op op, PathCompiler pathCompiler) {
        return Transformer.transform(new AltTransformer(pathCompiler), op);
    }

    private static class AltTransformer extends TransformCopy {
        private PathCompiler pathCompiler;

        public AltTransformer(PathCompiler pathCompiler) {
            this.pathCompiler = pathCompiler;
        }

        @Override
        public Op transform(OpPath opPath) {
            TriplePath triplePath = opPath.getTriplePath();
            Path path = triplePath.getPath();
            if (path instanceof P_Alt) {
                Node s = triplePath.getSubject();
                Node o = triplePath.getObject();
                Path pathLeft = ((P_Alt) path).getLeft();
                Op opLeft = null;
                if (pathLeft instanceof P_Link) {
                    opLeft = new OpTriple(Triple.create(s, ((P_Link) pathLeft).getNode(), o)).asBGP();
                } else {
                    opLeft = transformReduce(new OpPath(new TriplePath(s, pathLeft, o)), pathCompiler);
                }
                Path pathRight = ((P_Alt) path).getRight();
                Op opRight = null;
                if (pathRight instanceof P_Link) {
                    opRight = new OpTriple(Triple.create(s, ((P_Link) pathRight).getNode(), o)).asBGP();
                } else {
                    opRight = transformReduce(new OpPath(new TriplePath(s, pathRight, o)), pathCompiler);
                }
                return OpUnion.create(opLeft, opRight);
            }
            return super.transform(opPath);
        }
    }
}