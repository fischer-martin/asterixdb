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

import java.util.Map;

import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.evaluators.common.LabelTypeTuple;
import org.apache.asterix.runtime.evaluators.common.LabelTypeTupleFactory;
import org.apache.asterix.runtime.evaluators.common.MutableIntFactory;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Use {@link JSONTreeLabelTypeIntersectionVisitor} to recursively traverse pointables and to count the (label, type)-
 * tuples of their JSON trees.
 * Example: JSONTreeLabelTypeIntersectionVisitor jTreeLTIVisitor = new JSONTreeLabelTypeIntersectionVisitor();
 *          IVisitablePointable pointable = ...;
 *          pointable.accept(jTreeLTIVisitor, arg);
 */

public class JSONTreeLabelTypeIntersectionVisitor
        implements IVisitablePointableVisitor<Void, MutablePair<Map<LabelTypeTuple, MutableInt>, MutableInt>> {
    private final IObjectPool<LabelTypeTuple, ATypeTag> labelTypeTupleAllocator;
    private final IObjectPool<MutableInt, ATypeTag> mutableIntAllocator;

    public JSONTreeLabelTypeIntersectionVisitor() {
        // Use ListObjectPool to reuse in-memory buffers instead of allocating new memory for each (label, type).
        labelTypeTupleAllocator = new ListObjectPool<>(new LabelTypeTupleFactory());
        mutableIntAllocator = new ListObjectPool<>(new MutableIntFactory());
    }

    public void reset() {
        // Free in-memory buffers for reuse.
        labelTypeTupleAllocator.reset();
        mutableIntAllocator.reset();
    }

    private void addToBag(Map<LabelTypeTuple, MutableInt> bag, LabelTypeTuple ltt) {
        MutableInt count = bag.get(ltt);

        if (count == null) {
            count = mutableIntAllocator.allocate(null);
            count.setValue(1);
            bag.put(ltt, count);
        } else {
            count.increment();
        }
    }

    @Override
    public Void visit(AListVisitablePointable pointable, MutablePair<Map<LabelTypeTuple, MutableInt>, MutableInt> arg)
            throws HyracksDataException {
        // Create a new list (label, type) (array or multiset).
        LabelTypeTuple listLTT = labelTypeTupleAllocator.allocate(null);
        listLTT.setLabel(null);
        if (pointable.ordered()) {
            listLTT.setType(4);
        } else {
            listLTT.setType(5);
        }

        arg.right.increment();
        addToBag(arg.left, listLTT);

        // Recursively visit all list children (elements).
        for (int i = 0; i < pointable.getItems().size(); i++) {
            pointable.getItems().get(i).accept(this, arg);
        }

        return null;
    }

    @Override
    public Void visit(ARecordVisitablePointable pointable, MutablePair<Map<LabelTypeTuple, MutableInt>, MutableInt> arg)
            throws HyracksDataException {
        // Create a new object (label, type).
        LabelTypeTuple objectLTT = labelTypeTupleAllocator.allocate(null);
        objectLTT.setLabel(null);
        objectLTT.setType(3);

        arg.right.increment();
        addToBag(arg.left, objectLTT);

        // Recursively visit all object children (key-value pairs).
        for (int i = 0; i < pointable.getFieldValues().size(); i++) {
            pointable.getFieldValues().get(i).accept(this, arg);

            // Building a key (label, type).
            IVisitablePointable key = pointable.getFieldNames().get(i);
            LabelTypeTuple keyLTT = labelTypeTupleAllocator.allocate(null);
            keyLTT.setLabel(key);
            keyLTT.setType(2);

            arg.right.increment();
            addToBag(arg.left, keyLTT);;
        }

        return null;
    }

    @Override
    public Void visit(AFlatValuePointable pointable, MutablePair<Map<LabelTypeTuple, MutableInt>, MutableInt> arg)
            throws HyracksDataException {
        LabelTypeTuple ltt = labelTypeTupleAllocator.allocate(null);
        ltt.setLabel(pointable);
        ltt.setType(1);

        arg.right.increment();
        addToBag(arg.left, ltt);

        return null;
    }

}
