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

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.common.JSONCostModel;
import org.apache.asterix.runtime.evaluators.common.JSONTreeTransformator;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SizeDifferenceEvaluator implements IScalarEvaluator {
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

    private final MutableInt nodeCounter = new MutableInt();
    private final JSONTreeTransformator treeTransformator = new JSONTreeTransformator();
    private final JSONCostModel cm = new JSONCostModel(1, 1, 1); // Unit cost model (each operation has cost 1).

    public SizeDifferenceEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc, IAType type1, IAType type2) throws HyracksDataException {
        PointableAllocator allocator = new PointableAllocator();
        firstStringEval = args[0].createScalarEvaluator(context);
        secondStringEval = args[1].createScalarEvaluator(context);
        pointableLeft = allocator.allocateFieldValue(type1);
        pointableRight = allocator.allocateFieldValue(type2);
        this.sourceLoc = sourceLoc;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        firstStringEval.evaluate(tuple, pointableLeft);
        secondStringEval.evaluate(tuple, pointableRight);

        // count tree sizes
        nodeCounter.setValue(0);
        int t1Size = treeTransformator.calculateTreeSize(pointableLeft, nodeCounter);

        nodeCounter.setValue(0);
        int t2Size = treeTransformator.calculateTreeSize(pointableRight, nodeCounter);

        writeResult(jediLengthFilter(t1Size, t2Size));
        result.set(resultStorage);
    }

    protected void writeResult(double sizeDiff) throws HyracksDataException {
        aDouble.setValue(sizeDiff);
        doubleSerde.serialize(aDouble, out);
    }

    private double jediLengthFilter(double t1Size, double t2Size) {
        //return Math.abs(t1Size - t2Size); // for unit cost model
        // only works if the deletion/insertion costs are constants and don't get computed for a specific node
        if (t1Size >= t2Size) {
            return (t1Size - t2Size) * cm.del(null);
        } else {
            return (t2Size - t1Size) * cm.ins(null);
        }
    }
}
