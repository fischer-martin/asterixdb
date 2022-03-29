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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.flexiblejoin.cartilage.Configuration;
import org.apache.asterix.runtime.flexiblejoin.cartilage.FlexibleJoin;
import org.apache.asterix.runtime.flexiblejoin.oipjoin.FJInterval;
import org.apache.asterix.runtime.flexiblejoin.spatialjoin.Rectangle;
import org.apache.asterix.runtime.unnestingfunctions.base.AbstractUnnestingFunctionDynamicDescriptor;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FJAssignOneDescriptor extends AbstractUnnestingFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = FJAssignOneDescriptor::new;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.FJ_ASSIGN_ONE;
    }

    @Override
    public IUnnestingEvaluatorFactory createUnnestingEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IUnnestingEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IUnnestingEvaluator createUnnestingEvaluator(final IEvaluatorContext ctx)
                    throws HyracksDataException {

                return new IUnnestingEvaluator() {
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private int[] buckets;
                    private final IPointable inputArg0 = new VoidPointable();
                    private final IPointable inputArg1 = new VoidPointable();
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);
                    int pos;

                    private final AMutableInt32 aInt32 = new AMutableInt32(0);

                    @SuppressWarnings("rawtypes")
                    private ISerializerDeserializer serde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);

                    private Class<?> flexibleJoinClass = null;
                    {
                        try {
                            if (BuiltinFunctions.FJ_ASSIGN_ONE.getLibraryName().isEmpty()) {
                                BuiltinFunctions.FJ_ASSIGN_ONE
                                        .setLibraryName("org.apache.asterix.runtime.flexiblejoin.setsimilarity.SetSimilarityJoin");
                                List<Mutable<ILogicalExpression>> parameters = new ArrayList<>();
                                parameters.add(new MutableObject<>(
                                        new ConstantExpression(new AsterixConstantValue(new ADouble(0.5)))));
                                BuiltinFunctions.FJ_ASSIGN_ONE.setParameters(parameters);

                            }
                            flexibleJoinClass = Class.forName(BuiltinFunctions.FJ_ASSIGN_ONE.getLibraryName());
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    }

                    private FlexibleJoin flexibleJoin = null;
                    private Configuration configuration = null;
                    private List<Mutable<ILogicalExpression>> parameters =
                            BuiltinFunctions.FJ_ASSIGN_ONE.getParameters();

                    @Override
                    public void init(IFrameTupleReference tuple) throws HyracksDataException {
                        eval0.evaluate(tuple, inputArg0);
                        eval1.evaluate(tuple, inputArg1);

                        byte[] bytes0 = inputArg0.getByteArray();
                        byte[] bytes1 = inputArg1.getByteArray();

                        int offset0 = inputArg0.getStartOffset();
                        int offset1 = inputArg1.getStartOffset();

                        int len = inputArg0.getLength();
                        int len1 = inputArg1.getLength();

                        ATypeTag tag0 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]);

                        if (flexibleJoin == null) {
                            AlgebricksConfig.ALGEBRICKS_LOGGER.info(
                                    "FJ ASSIGN ONE: ID: " + ctx.getServiceContext().getControllerService().getId());
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
                            ByteArrayInputStream inStream1 = new ByteArrayInputStream(bytes1, offset1, len1 + 1);
                            DataInputStream dataIn1 = new DataInputStream(inStream1);
                            configuration = SerializationUtils.deserialize(dataIn1);
                        }

                        if (tag0 == ATypeTag.STRING) {
                            ByteArrayInputStream inStream =
                                    new ByteArrayInputStream(inputArg0.getByteArray(), offset0 + 1, len - 1);
                            DataInputStream dataIn = new DataInputStream(inStream);
                            String key = AStringSerializerDeserializer.INSTANCE.deserialize(dataIn).getStringValue();
                            buckets = flexibleJoin.assign1(key, configuration);
                        } else if (tag0 == ATypeTag.RECTANGLE) {
                            double minX = ADoubleSerializerDeserializer.getDouble(bytes0, offset0 + 1
                                    + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                            double minY = ADoubleSerializerDeserializer.getDouble(bytes0, offset0 + 1
                                    + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));
                            double maxX = ADoubleSerializerDeserializer.getDouble(bytes0, offset0 + 1
                                    + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
                            double maxY = ADoubleSerializerDeserializer.getDouble(bytes0, offset0 + 1
                                    + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));

                            Rectangle key = new Rectangle(minX, maxX, minY, maxY);
                            buckets = flexibleJoin.assign1(key, configuration);
                        } else if (tag0 == ATypeTag.INTERVAL) {
                            long start = AIntervalSerializerDeserializer.getIntervalStart(bytes0, offset0+1);
                            long end = AIntervalSerializerDeserializer.getIntervalEnd(bytes0, offset0+1);

                            FJInterval fjInterval = new FJInterval(start, end);
                            buckets = flexibleJoin.assign1(fjInterval, configuration);
                        }
                        pos = 0;

                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public boolean step(IPointable result) throws HyracksDataException {
                        if (pos >= buckets.length) {
                            return false;
                        }
                        aInt32.setValue(buckets[pos]);
                        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isDebugEnabled()) {
                            AlgebricksConfig.ALGEBRICKS_LOGGER.info("Assign One step : " + buckets[pos] + " ID: "
                                    + ctx.getServiceContext().getControllerService().getId() + ".\n");
                        }
                        resultStorage.reset();
                        serde.serialize(aInt32, resultStorage.getDataOutput());
                        result.set(resultStorage);
                        ++pos;
                        return true;
                    }
                };
            }
        };
    }
}
