/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.evaluators.visitors;

import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Use {@link JSONTreeSizeVisitor} to recursively traverse pointables and to calculate the size of their JSON trees.
 * Example: JSONTreeSizeVisitor jTreeSizeVisitor = new JSONTreeSizeVisitor();
 *          IVisitablePointable pointable = ...;
 *          pointable.accept(jTreeSizeVisitor, nodeCounter);
 */

public class JSONTreeSizeVisitor implements IVisitablePointableVisitor<Void, MutableInt> {

    @Override
    public Void visit(AListVisitablePointable pointable, MutableInt nodeCounter) throws HyracksDataException {
        // list node (array or multiset)
        nodeCounter.increment();

        // Recursively visit all list children (elements).
        for (int i = 0; i < pointable.getItems().size(); i++) {
            pointable.getItems().get(i).accept(this, nodeCounter);
        }

        return null;
    }

    @Override
    public Void visit(ARecordVisitablePointable pointable, MutableInt nodeCounter) throws HyracksDataException {
        // object node
        nodeCounter.increment();

        // Recursively visit all object children (key-value pairs).
        for (int i = 0; i < pointable.getFieldValues().size(); i++) {
            // key node
            nodeCounter.increment();

            // key node's child
            pointable.getFieldValues().get(i).accept(this, nodeCounter);
        }

        return null;
    }

    @Override
    public Void visit(AFlatValuePointable pointable, MutableInt nodeCounter) throws HyracksDataException {
        // literal node
        nodeCounter.increment();

        return null;
    }

}
