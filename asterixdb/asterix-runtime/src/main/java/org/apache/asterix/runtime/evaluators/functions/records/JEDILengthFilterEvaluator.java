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
import java.util.List;

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
import org.apache.asterix.runtime.evaluators.common.JSONCostModel;
import org.apache.asterix.runtime.evaluators.common.JSONTreeTransformator;
import org.apache.asterix.runtime.evaluators.common.Node;
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

import it.unimi.dsi.fastutil.ints.IntIntMutablePair;
import it.unimi.dsi.fastutil.ints.IntIntPair;

public class JEDILengthFilterEvaluator implements IScalarEvaluator {
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
    private final IObjectPool<List<Node>, ATypeTag> listAllocator;

    MutablePair<List<Node>, IntIntPair> transArg = new MutablePair<>();
    IntIntPair transCnt = new IntIntMutablePair(0, 0);
    private final JSONTreeTransformator treeTransformator = new JSONTreeTransformator();
    private final JSONCostModel cm = new JSONCostModel(1, 1, 1); // Unit cost model (each operation has cost 1).

    public JEDILengthFilterEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc, IAType type1, IAType type2) throws HyracksDataException {
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

        // TODO: custom visitor that only counts a tree's size instead of actually constructing the tree

        // Convert the given data items into JSON trees.
        List<Node> postToNode1 = listAllocator.allocate(null);
        postToNode1.clear();
        transArg.setLeft(postToNode1);
        transCnt.first(0);
        transCnt.second(0);
        transArg.setRight(transCnt);
        postToNode1 = treeTransformator.toTree(pointableLeft, transArg);

        List<Node> postToNode2 = listAllocator.allocate(null);
        postToNode2.clear();
        transArg.setLeft(postToNode2);
        transCnt.first(0);
        transCnt.second(0);
        transArg.setRight(transCnt);
        postToNode2 = treeTransformator.toTree(pointableRight, transArg);

        writeResult(jediLengthFilter(postToNode1.size(), postToNode2.size()));
        result.set(resultStorage);
    }

    protected void writeResult(double length_filter) throws HyracksDataException {
        aDouble.setValue(length_filter);
        doubleSerde.serialize(aDouble, out);
    }

    private double jediLengthFilter(double t1_size, double t2_size) {
        //return Math.abs(t1_size - t2_size); // for unit cost model
        // only works if the deletion/insertion costs are constants and don't get computed for a specific node
        if (t1_size >= t2_size) {
            return (t1_size - t2_size) * cm.del(null);
        } else
            return (t2_size - t1_size) * cm.ins(null);
    }
}
