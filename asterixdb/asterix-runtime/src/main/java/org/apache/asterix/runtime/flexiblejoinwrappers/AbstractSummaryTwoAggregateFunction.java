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
package org.apache.asterix.runtime.flexiblejoinwrappers;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.aggregates.std.AbstractAggregateFunction;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.asterix.runtime.flexiblejoin.Summary;
import org.apache.asterix.runtime.flexiblejoin.WordCount;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractSummaryTwoAggregateFunction extends AbstractAggregateFunction {

    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private IPointable inputVal = new VoidPointable();
    private final IScalarEvaluator eval;
    protected final IEvaluatorContext context;
    private String libraryName = "";
    private Summary summary;
    Type type;
    protected ATypeTag aggType;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ANull> nullSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);

    public AbstractSummaryTwoAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(sourceLoc);
        this.eval = args[0].createScalarEvaluator(context);
        this.context = context;
        this.libraryName = BuiltinFunctions.SCALAR_FJ_SUMMARY_TWO.getLibraryName();

        Type[] genericInterfaces = WordCount.class.getGenericInterfaces();
        type = (((ParameterizedType) genericInterfaces[0]).getActualTypeArguments()[0]);
        this.summary = new WordCount();
    }

    @Override
    public void init() throws HyracksDataException {
    }

    @Override
    public abstract void step(IFrameTupleReference tuple) throws HyracksDataException;

    @Override
    public abstract void finish(IPointable result) throws HyracksDataException;

    @Override
    public abstract void finishPartial(IPointable result) throws HyracksDataException;

    protected void processNull(ATypeTag typeTag) throws UnsupportedItemTypeException {
        throw new UnsupportedItemTypeException(sourceLoc, BuiltinFunctions.FJ_SUMMARY_TWO, typeTag.serialize());
    }

    public void processDataValues(IFrameTupleReference tuple) throws HyracksDataException {
        eval.evaluate(tuple, inputVal);
        byte[] data = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();
        int len = inputVal.getLength();

        //System.out.println(offset);
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
        aggType = typeTag;

        if (typeTag == ATypeTag.NULL || typeTag == ATypeTag.MISSING) {
            processNull(typeTag);
        } else {
            ByteArrayInputStream inStream = new ByteArrayInputStream(data, offset + 1, len - 1);
            DataInputStream dataIn = new DataInputStream(inStream);

            if (typeTag == ATypeTag.STRING && type.equals(String.class)) {
                String key = AStringSerializerDeserializer.INSTANCE.deserialize(dataIn).getStringValue();
                summary.add(key);
            }
        }
    }

    public void processPartialResults(IFrameTupleReference tuple) throws IOException {

        eval.evaluate(tuple, inputVal);
        byte[] data = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();
        int len = inputVal.getLength();

        //ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
        //int nullBitmapSize = 0;
        //int offset1 = ARecordSerializerDeserializer.getFieldOffsetById(data, offset, 0,
        //        nullBitmapSize, false);
        //int len = ARecordSerializerDeserializer.getRecordLength(data, 0);


        //System.out.println(dataIn.readAllBytes().toString());
        //String key = AStringSerializerDeserializer.INSTANCE.deserialize(dataIn).getStringValue();
        try {
            ByteArrayInputStream inStream = new ByteArrayInputStream(data, offset, len+1);
            DataInputStream dataIn = new DataInputStream(inStream);

            Summary<String> s = SerializationUtils.deserialize(dataIn);
            summary.add(s);

        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }

    }

    protected void finishPartialResults(IPointable result) throws HyracksDataException {
        finishFinalResults(result);
    }

    protected void finishFinalResults(IPointable result) throws HyracksDataException {
        resultStorage.reset();
        try {
            resultStorage.getDataOutput().write(SerializationUtils.serialize(this.summary));

        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(resultStorage);
    }

    protected boolean skipStep() {
        return false;
    }
}
