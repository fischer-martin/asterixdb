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
import java.util.HashMap;import java.util.List;

import org.apache.asterix.builders.ArrayListFactory;
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
import org.apache.commons.lang3.mutable.MutableInt;import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import it.unimi.dsi.fastutil.ints.IntIntMutablePair;
import it.unimi.dsi.fastutil.ints.IntIntPair;

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
    private final IObjectPool<List<Node>, ATypeTag> listAllocator; // TODO replace this with an IObjectPool for HashMap

    private final JSONTreeTransformator treeTransformator = new JSONTreeTransformator();

    public IntersectionLowerBoundEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext context, SourceLocation sourceLoc,
            IAType type1, IAType type2) throws HyracksDataException {
        PointableAllocator allocator = new PointableAllocator();
        firstStringEval = args[0].createScalarEvaluator(context);
        secondStringEval = args[1].createScalarEvaluator(context);
        pointableLeft = allocator.allocateFieldValue(type1);
        pointableRight = allocator.allocateFieldValue(type2);
        listAllocator = new ListObjectPool<>(new ArrayListFactory<Node>());
        this.sourceLoc = sourceLoc;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        treeTransformator.reset();
        listAllocator.reset();
        firstStringEval.evaluate(tuple, pointableLeft);
        secondStringEval.evaluate(tuple, pointableRight);

        // Convert the given data items into node bags of their JSON trees.
        HashMap<LabelTypeTuple, MutableInt> nodeBag1 = treeTransformator.generateNodeBag(pointableLeft, new HashMap<>());
        HashMap<LabelTypeTuple, MutableInt> nodeBag2 = treeTransformator.generateNodeBag(pointableLeft, new HashMap<>());

        writeResult(intersectionLowerBound(nodeBag1, nodeBag2));
        result.set(resultStorage);
    }

    protected void writeResult(double distance) throws HyracksDataException {
        aDouble.setValue(distance);
        doubleSerde.serialize(aDouble, out);
    }

    private double intersectionLowerBound(HashMap<LabelTypeTuple, MutableInt> bag1, HashMap<LabelTypeTuple, MutableInt> bag2) {
        // TODO
        return -1;
    }

}
