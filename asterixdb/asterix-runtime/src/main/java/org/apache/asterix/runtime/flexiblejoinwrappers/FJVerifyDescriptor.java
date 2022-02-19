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
import java.util.List;

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.flexiblejoin.Configuration;
import org.apache.asterix.runtime.flexiblejoin.FlexibleJoin;
import org.apache.asterix.runtime.flexiblejoin.Rectangle;
import org.apache.asterix.runtime.flexiblejoin.SetSimilarityJoin;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FJVerifyDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = () -> new FJVerifyDescriptor();

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.FJ_VERIFY;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {

        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {

                return new IScalarEvaluator() {

                    private Class<?> flexibleJoinClass = null;
                    {
                        try {
                            if(BuiltinFunctions.FJ_VERIFY.getLibraryName().isEmpty()) {
                                BuiltinFunctions.FJ_VERIFY.setLibraryName("org.apache.asterix.runtime.flexiblejoin.SetSimilarityJoin");

                            }
                            flexibleJoinClass = Class.forName(BuiltinFunctions.FJ_VERIFY.getLibraryName());
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    }

                    private FlexibleJoin flexibleJoin = null;
                    private Configuration configuration = null;
                    private List<Mutable<ILogicalExpression>> parameters = BuiltinFunctions.FJ_VERIFY.getParameters();

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final IPointable inputArg0 = new VoidPointable();
                    private final IPointable inputArg1 = new VoidPointable();
                    private final IPointable inputArg2 = new VoidPointable();
                    private final IPointable inputArg3 = new VoidPointable();
                    private final IPointable inputArg4 = new VoidPointable();

                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval2 = args[2].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval3 = args[3].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval4 = args[4].createScalarEvaluator(ctx);

                    private final AStringSerializerDeserializer serde = AStringSerializerDeserializer.INSTANCE;

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();

                        eval0.evaluate(tuple, inputArg0);
                        eval1.evaluate(tuple, inputArg1);
                        eval2.evaluate(tuple, inputArg2);
                        eval3.evaluate(tuple, inputArg3);
                        eval4.evaluate(tuple, inputArg4);

                        byte[] bytes0 = inputArg0.getByteArray();
                        byte[] bytes1 = inputArg1.getByteArray();
                        byte[] bytes2 = inputArg2.getByteArray();
                        byte[] bytes3 = inputArg3.getByteArray();
                        byte[] bytes4 = inputArg4.getByteArray();

                        int offset0 = inputArg0.getStartOffset();
                        int offset1 = inputArg1.getStartOffset();
                        int offset2 = inputArg2.getStartOffset();
                        int offset3 = inputArg3.getStartOffset();
                        int offset4 = inputArg4.getStartOffset();

                        int len3 = inputArg3.getLength();
                        int len1 = inputArg1.getLength();
                        int len4 = inputArg4.getLength();
                        boolean verifyResult = false;
                        try {

                            int bucketID0 = AInt32SerializerDeserializer.getInt(bytes0, offset0 + 1);
                            int bucketID1 = AInt32SerializerDeserializer.getInt(bytes2, offset2 + 1);

                            if (flexibleJoin == null) {
                                AlgebricksConfig.ALGEBRICKS_LOGGER
                                        .info("FJ VERIFY: ID: " + ctx.getServiceContext().getControllerService().getId());

                                //ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
                                Constructor<?> flexibleJoinConstructer = flexibleJoinClass.getConstructors()[0];
                                if (parameters != null) {
                                    ConstantExpression c = (ConstantExpression) parameters.get(0).getValue();
                                    IAlgebricksConstantValue d = c.getValue();
                                    Double dx = Double.valueOf(d.toString());
                                    flexibleJoin = (FlexibleJoin) flexibleJoinConstructer.newInstance(dx);
                                } else {
                                    flexibleJoin = (FlexibleJoin) flexibleJoinConstructer.newInstance();
                                }

                                ByteArrayInputStream inStream4 = new ByteArrayInputStream(bytes4, offset4, len4 + 1);
                                DataInputStream dataIn4 = new DataInputStream(inStream4);

                                configuration = SerializationUtils.deserialize(dataIn4);

                                AlgebricksConfig.ALGEBRICKS_LOGGER
                                        .info("MATCH COUNTER:" + SetSimilarityJoin.matchCounter);
                            }

                            ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]);

                            if (tag == ATypeTag.STRING) {
                                ByteArrayInputStream inStream1 =
                                        new ByteArrayInputStream(inputArg1.getByteArray(), offset1 + 1, len1 - 1);
                                DataInputStream dataIn1 = new DataInputStream(inStream1);
                                String key0 = serde.deserialize(dataIn1).getStringValue();

                                ByteArrayInputStream inStream3 =
                                        new ByteArrayInputStream(inputArg3.getByteArray(), offset3 + 1, len3 - 1);
                                DataInputStream dataIn3 = new DataInputStream(inStream3);
                                String key1 = serde.deserialize(dataIn3).getStringValue();

                                verifyResult = flexibleJoin.verify(bucketID0, key0, bucketID1, key1, configuration);
                            } else if (tag == ATypeTag.RECTANGLE) {
                                double minX1 = ADoubleSerializerDeserializer.getDouble(bytes1, offset1 + 1
                                        + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                                double minY1 = ADoubleSerializerDeserializer.getDouble(bytes1, offset1 + 1
                                        + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));
                                double maxX1 = ADoubleSerializerDeserializer.getDouble(bytes1, offset1 + 1
                                        + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
                                double maxY1 = ADoubleSerializerDeserializer.getDouble(bytes1, offset1 + 1
                                        + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));

                                Rectangle key0 = new Rectangle(minX1, maxX1, minY1, maxY1);

                                double minX2 = ADoubleSerializerDeserializer.getDouble(bytes3, offset3 + 1
                                        + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                                double minY2 = ADoubleSerializerDeserializer.getDouble(bytes3, offset3 + 1
                                        + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));
                                double maxX2 = ADoubleSerializerDeserializer.getDouble(bytes3, offset3 + 1
                                        + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
                                double maxY2 = ADoubleSerializerDeserializer.getDouble(bytes3, offset3 + 1
                                        + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));

                                Rectangle key1 = new Rectangle(minX2, maxX2, minY2, maxY2);

                                verifyResult = flexibleJoin.verify(bucketID0, key0, bucketID1, key1, configuration);

                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        try {
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN)
                                    .serialize(verifyResult ? ABoolean.TRUE : ABoolean.FALSE,
                                            resultStorage.getDataOutput());

                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }
}
