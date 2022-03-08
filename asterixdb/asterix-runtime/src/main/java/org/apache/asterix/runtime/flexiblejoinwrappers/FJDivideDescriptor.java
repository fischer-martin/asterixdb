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
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.flexiblejoin.Configuration;
import org.apache.asterix.runtime.flexiblejoin.FlexibleJoin;
import org.apache.asterix.runtime.flexiblejoin.Summary;
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
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FJDivideDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = () -> new FJDivideDescriptor();

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.FJ_DIVIDE;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {

                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final IPointable inputArg0 = new VoidPointable();
                    private final IPointable inputArg1 = new VoidPointable();
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);

                    private Class<?> flexibleJoinClass = null;
                    {
                        try {
                            if (BuiltinFunctions.FJ_DIVIDE.getLibraryName().isEmpty()) {
                                BuiltinFunctions.FJ_DIVIDE
                                        .setLibraryName("org.apache.asterix.runtime.flexiblejoin.SetSimilarityJoin");
                                List<Mutable<ILogicalExpression>> parameters = new ArrayList<>();
                                parameters.add(new MutableObject<>(
                                        new ConstantExpression(new AsterixConstantValue(new ADouble(0.5)))));
                                BuiltinFunctions.FJ_DIVIDE.setParameters(parameters);

                            }
                            flexibleJoinClass = Class.forName(BuiltinFunctions.FJ_DIVIDE.getLibraryName());
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    }

                    private FlexibleJoin flexibleJoin = null;
                    private Configuration configuration = null;
                    private List<Mutable<ILogicalExpression>> parameters = BuiltinFunctions.FJ_DIVIDE.getParameters();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        AlgebricksConfig.ALGEBRICKS_LOGGER
                                .info("FJ DIVIDE: ID: " + ctx.getServiceContext().getControllerService().getId());
                        resultStorage.reset();

                        eval0.evaluate(tuple, inputArg0);
                        eval1.evaluate(tuple, inputArg1);

                        //if (PointableHelper.checkAndSetMissingOrNull(result, inputArg0, inputArg1)) {
                        //    return;
                        //}

                        byte[] bytes0 = inputArg0.getByteArray();
                        byte[] bytes1 = inputArg1.getByteArray();

                        int offset0 = inputArg0.getStartOffset();
                        int offset1 = inputArg1.getStartOffset();

                        int len0 = inputArg0.getLength();
                        int len1 = inputArg1.getLength();

                        ByteArrayInputStream inStream0 = new ByteArrayInputStream(bytes0, offset0, len0 + 1);
                        DataInputStream dataIn0 = new DataInputStream(inStream0);

                        ByteArrayInputStream inStream1 = new ByteArrayInputStream(bytes1, offset1, len1 + 1);
                        DataInputStream dataIn1 = new DataInputStream(inStream1);

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
                        }

                        Summary summaryOne = SerializationUtils.deserialize(dataIn0);
                        Summary summaryTwo = SerializationUtils.deserialize(dataIn1);

                        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isDebugEnabled()) {
                            AlgebricksConfig.ALGEBRICKS_LOGGER
                                    .info("\nFJ DIVIDE: ID: " + ctx.getServiceContext().getControllerService().getId());
                        }
                        Configuration C = (Configuration) flexibleJoin.divide(summaryOne, summaryTwo);
                        try {
                            resultStorage.getDataOutput().write(SerializationUtils.serialize(C));
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
