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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.aggregates.std.AbstractAggregateFunction;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.asterix.runtime.flexiblejoin.FlexibleJoin;
import org.apache.asterix.runtime.flexiblejoin.Rectangle;
import org.apache.asterix.runtime.flexiblejoin.Summary;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
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

public abstract class AbstractSummaryOneAggregateFunction extends AbstractAggregateFunction {

    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private IPointable inputVal = new VoidPointable();
    private final IScalarEvaluator eval;
    protected final IEvaluatorContext context;
    private Summary summary;
    Type type;

    private Class<?> flexibleJoinClass = null;
    {
        try {
            if (BuiltinFunctions.FJ_SUMMARY_ONE.getLibraryName().isEmpty()) {
                BuiltinFunctions.FJ_SUMMARY_ONE
                        .setLibraryName("org.apache.asterix.runtime.flexiblejoin.SetSimilarityJoin");
                List<Mutable<ILogicalExpression>> parameters = new ArrayList<>();
                parameters.add(new MutableObject<>(
                        new ConstantExpression(new AsterixConstantValue(new ADouble(0.5)))));
                BuiltinFunctions.FJ_SUMMARY_ONE.setParameters(parameters);

            }
            flexibleJoinClass = Class.forName(BuiltinFunctions.FJ_SUMMARY_ONE.getLibraryName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private FlexibleJoin flexibleJoin = null;
    private List<Mutable<ILogicalExpression>> parameters = BuiltinFunctions.FJ_SUMMARY_ONE.getParameters();

    protected ATypeTag aggType;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ANull> nullSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);

    public AbstractSummaryOneAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(sourceLoc);
        this.eval = args[0].createScalarEvaluator(context);
        this.context = context;

        if (flexibleJoin == null) {
            //ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
            Constructor<?> flexibleJoinConstructer = flexibleJoinClass.getConstructors()[0];
            if (parameters != null) {
                ConstantExpression c = (ConstantExpression) parameters.get(0).getValue();
                IAlgebricksConstantValue d = c.getValue();
                Double dx = Double.valueOf(d.toString());
                try {
                    flexibleJoin = (FlexibleJoin) flexibleJoinConstructer.newInstance(dx);
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    flexibleJoin = (FlexibleJoin) flexibleJoinConstructer.newInstance();
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
            this.summary = flexibleJoin.createSummarizer1();
        }

        Type[] genericInterfaces = flexibleJoinClass.getGenericInterfaces();
        type = (((ParameterizedType) genericInterfaces[0]).getActualTypeArguments()[0]);
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
                if (AlgebricksConfig.ALGEBRICKS_LOGGER.isDebugEnabled()) {
                    AlgebricksConfig.ALGEBRICKS_LOGGER.info("Process Data Summary One: " + key + " ID: "
                            + context.getServiceContext().getControllerService().getId() + ".\n");
                }
            } else if (typeTag == ATypeTag.RECTANGLE) {
                double minX = ADoubleSerializerDeserializer.getDouble(data,
                        offset + 1 + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                double minY = ADoubleSerializerDeserializer.getDouble(data,
                        offset + 1 + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));
                double maxX = ADoubleSerializerDeserializer.getDouble(data,
                        offset + 1 + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
                double maxY = ADoubleSerializerDeserializer.getDouble(data,
                        offset + 1 + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));

                Rectangle key = new Rectangle(minX, maxX, minY, maxY);
                summary.add(key);
            }
        }
    }

    public void processPartialResults(IFrameTupleReference tuple) throws IOException {
        eval.evaluate(tuple, inputVal);
        byte[] data = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();
        int len = inputVal.getLength();

        //int nullBitmapSize = 0;
        //int offset1 = ARecordSerializerDeserializer.getFieldOffsetById(data, offset, 0,
        //        nullBitmapSize, false);
        //int len = ARecordSerializerDeserializer.getRecordLength(data, 0);

        //ByteArrayInputStream inStream = new ByteArrayInputStream(data, offset + 1, len + 1);
        //DataInputStream dataIn = new DataInputStream(inStream);
        //System.out.println(dataIn.readAllBytes().toString());
        //String key = AStringSerializerDeserializer.INSTANCE.deserialize(dataIn).getStringValue();
        try {
            ByteArrayInputStream inStream = new ByteArrayInputStream(data, offset, len + 1);
            DataInputStream dataIn = new DataInputStream(inStream);
            Summary<String> s = SerializationUtils.deserialize(dataIn);
            summary.add(s);
            if (AlgebricksConfig.ALGEBRICKS_LOGGER.isDebugEnabled()) {
                AlgebricksConfig.ALGEBRICKS_LOGGER.info("Process Partial Summary One ID: "
                        + context.getServiceContext().getControllerService().getId() + ".\n");
            }

        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }

    }

    protected void finishPartialResults(IPointable result) throws HyracksDataException {
        AlgebricksConfig.ALGEBRICKS_LOGGER.info(
                "Finish Partial Summary One ID: " + context.getServiceContext().getControllerService().getId() + ".\n");
        finishFinalResults(result);
    }

    protected void finishFinalResults(IPointable result) throws HyracksDataException {
        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isDebugEnabled()) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.info("Finish Final Summary One ID: "
                    + context.getServiceContext().getControllerService().getId() + ".\n");
        }
        resultStorage.reset();
        try {
            resultStorage.getDataOutput().write(SerializationUtils.serialize(summary));
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(resultStorage);
    }

    protected boolean skipStep() {
        return false;
    }

    protected void processNull(ATypeTag typeTag) throws UnsupportedItemTypeException {
        throw new UnsupportedItemTypeException(sourceLoc, BuiltinFunctions.FJ_SUMMARY_TWO, typeTag.serialize());
    }
}
