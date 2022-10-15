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
package org.apache.asterix.runtime.evaluators.functions.records;

import java.io.DataOutput;
import java.util.Map;

import org.apache.asterix.builders.HashMapFactory;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.evaluators.common.*;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class IntersectionLowerBoundEvaluator implements IScalarEvaluator {
    protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected final DataOutput out = resultStorage.getDataOutput();
    protected final IScalarEvaluator firstStringEval;
    protected final IScalarEvaluator secondStringEval;
    protected final SourceLocation sourceLoc;
    protected final AMutableDouble aDouble = new AMutableDouble(-1.0);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AMutableDouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    private final IVisitablePointable pointableLeft;
    private final IVisitablePointable pointableRight;
    private final IObjectPool<Map<LabelTypeTuple, MutableInt>, ATypeTag> mapAllocator;
    private final MutablePair<Map<LabelTypeTuple, MutableInt>, MutableInt> transArg = new MutablePair<>(null, new MutableInt());

    private final JSONTreeTransformator treeTransformator = new JSONTreeTransformator();

    public IntersectionLowerBoundEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc, IAType type1, IAType type2) throws HyracksDataException {
        PointableAllocator allocator = new PointableAllocator();
        firstStringEval = args[0].createScalarEvaluator(context);
        secondStringEval = args[1].createScalarEvaluator(context);
        pointableLeft = allocator.allocateFieldValue(type1);
        pointableRight = allocator.allocateFieldValue(type2);
        mapAllocator = new ListObjectPool<>(new HashMapFactory<>());
        this.sourceLoc = sourceLoc;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        treeTransformator.reset();
        mapAllocator.reset();
        firstStringEval.evaluate(tuple, pointableLeft);
        secondStringEval.evaluate(tuple, pointableRight);

        // Convert the given data items into node bags of their JSON trees.
        Map<LabelTypeTuple, MutableInt> nodeBag1 = mapAllocator.allocate(null);
        nodeBag1.clear();
        transArg.setLeft(nodeBag1);
        transArg.right.setValue(0);
        MutablePair<Map<LabelTypeTuple, MutableInt>, MutableInt> temp = treeTransformator
                .countAndGenerateNodeBag(pointableLeft, transArg);
        nodeBag1 = temp.left;
        int treeSize1 = temp.right.intValue();

        Map<LabelTypeTuple, MutableInt> nodeBag2 = mapAllocator.allocate(null);
        nodeBag2.clear();
        transArg.setLeft(nodeBag2);
        transArg.right.setValue(0);
        temp = treeTransformator.countAndGenerateNodeBag(pointableRight, transArg);
        nodeBag2 = temp.left;
        int treeSize2 = temp.right.intValue();

        writeResult(intersectionLowerBound(treeSize1, nodeBag1, treeSize2, nodeBag2));
        result.set(resultStorage);
    }

    protected void writeResult(double lowerBound) throws HyracksDataException {
        aDouble.setValue(lowerBound);
        doubleSerde.serialize(aDouble, out);
    }

    private double intersectionLowerBound(int treeSize1, Map<LabelTypeTuple, MutableInt> bag1, int treeSize2,
            Map<LabelTypeTuple, MutableInt> bag2) {
        int intersectionSize = 0;

        MutableInt otherCount;
        for (var entry : bag1.entrySet()) {
            otherCount = bag2.get(entry.getKey());

            if (otherCount != null) {
                intersectionSize += Math.min(entry.getValue().intValue(), otherCount.intValue());
            }
        }

        return Math.max(treeSize1, treeSize2) - intersectionSize;
    }

}
