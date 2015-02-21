/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.dboe.trans.bplustree;

import java.util.* ;

import org.apache.jena.atlas.iterator.Iter ;
import org.apache.jena.atlas.lib.InternalErrorException ;
import org.seaborne.dboe.base.record.Record ;
import org.seaborne.dboe.trans.bplustree.AccessPath.AccessStep ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

/** Iterator over records that does not assume records block linkage */ 
class BPTreeRangeIterator implements Iterator<Record> {
    static Logger log = LoggerFactory.getLogger(BPTreeRangeIterator.class) ;
    
    public static Iterator<Record> create(BPTreeNode node, Record minRec, Record maxRec) {
        if ( minRec != null && maxRec != null && Record.keyGE(minRec, maxRec) )
            return Iter.nullIter();
        return new BPTreeRangeIterator(node, minRec, maxRec) ;
    }
    
    // Convert path to a stack of iterators
    private final Deque<Iterator<BPTreePage>> stack = new ArrayDeque<>();
    final private Record minRecord ;
    final private Record maxRecord ;
    private Iterator<Record> current ;
    private Record slot = null ;
    private boolean finished = false ;
    
    BPTreeRangeIterator(BPTreeNode node, Record minRec, Record maxRec ) {
        this.minRecord = minRec ;
        this.maxRecord = maxRec ;
        BPTreeRecords r = loadStack(node) ;
        current = r.getRecordBuffer().iterator(minRecord, maxRecord) ;
    }

    @Override
    public boolean hasNext() {
        if ( finished ) 
            return false ;
        if ( slot != null )
            return true ;
//        if ( current != null && current.hasNext() ) {
//            slot = current.next() ;
//            return true ;
//        }
        
        while(current != null && !current.hasNext()) {
            current = moveOnCurrent() ;
        } 
        if ( current == null ) {
            end() ;
            return false ;
        }
        slot = current.next() ;
        return true ;
        
    }
    
    // Move across the head of the stack until empty - then move next level. 
    private Iterator<Record> moveOnCurrent() {
        Iterator<BPTreePage> iter = null ;
        while(!stack.isEmpty()) { 
            iter = stack.peek() ;
            if ( iter.hasNext() )
              break ;
            stack.pop() ;
        } 
        
        if ( iter == null || ! iter.hasNext() )
            return null ;
        BPTreePage p = iter.next() ;
        BPTreeRecords r = null ;
        if (p instanceof BPTreeNode) {
            r = loadStack((BPTreeNode)p) ;
//            if ( logging(log) ) {
//                log(log, "moveOnCurrent: Node: %s", p.label()) ;
//                log(log, "moveOnCurrent:     r="+r.label()) ;
//            }
        }
        else {
//            if ( logging(log) )
//                log(log, "moveOnCurrent: Records: "+p.label()) ;
            r = (BPTreeRecords)p ;
        }
        return r.getRecordBuffer().iterator(minRecord, maxRecord) ;
    }
    
    private BPTreeRecords loadStack(BPTreeNode node) {
        AccessPath path = new AccessPath(null) ;
        if ( minRecord == null )
            node.internalMinRecord(path) ;
        else
            node.internalSearch(path, minRecord) ;
//        if ( logging(log) )
//            log(log, "loadStack: node: %s", node.label()) ;
        
        List<AccessStep> steps = path.getPath() ;
//        if ( logging(log) )
//            log(log, "loadStack: path = "+path) ;
        for ( AccessStep step : steps ) {
//            if ( logging(log) )
//                log(log, "           step = "+step) ;
            BPTreeNode n = step.node ; 
            Iterator<BPTreePage> it = n.iterator(minRecord, maxRecord) ;
            if ( it == null || ! it.hasNext() )
                continue ;
            // Drop the first  
            // TODO Why??
            BPTreePage p = it.next() ;
//            if ( logging(log) )
//                log(log, "           drop: %s", p.label()) ;
            stack.push(it) ;
//            if ( logging(log) )
//                log(log, "loadStack: push : "+n.label()) ;
        }
        
        BPTreePage p = steps.get(steps.size()-1).page ;
        if ( ! ( p instanceof BPTreeRecords ) )
            throw new InternalErrorException("Last path step not to a records block") ;
        return (BPTreeRecords)p ;
    }

    private void end() {
        finished = true ;
        current = null ;
    }
    
    @Override
    public Record next() {
        if ( ! hasNext() )
            throw new NoSuchElementException() ;
        Record r = slot ;
        if ( r == null )
            throw new InternalErrorException("Null slot after hasNext is true") ;
        slot = null ;
//        if ( logging(log) )
//            log(log, "Yield %s", r) ; 
        return r ;
    }
}
