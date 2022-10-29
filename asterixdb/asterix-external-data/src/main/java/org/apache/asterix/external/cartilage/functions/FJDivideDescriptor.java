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
package org.apache.asterix.external.cartilage.functions;

import static org.apache.asterix.external.cartilage.util.FlexibleJoinLoader.getFlexibleJoin;
import static org.apache.asterix.external.cartilage.util.FlexibleJoinLoader.getFlexibleJoinClassLoader;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.lang.reflect.InvocationTargetException;

import org.apache.asterix.external.cartilage.base.Configuration;
import org.apache.asterix.external.cartilage.base.FlexibleJoin;
import org.apache.asterix.external.cartilage.base.Summary;
import org.apache.asterix.external.cartilage.util.ClassLoaderAwareObjectInputStream;
import org.apache.asterix.om.functions.ExternalFJFunctionInfo;
import org.apache.asterix.om.functions.IExternalFunctionDescriptor;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.commons.lang3.SerializationUtils;
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

public class FJDivideDescriptor extends AbstractScalarFunctionDynamicDescriptor implements IExternalFunctionDescriptor {
    private static final long serialVersionUID = 1L;

    private final IExternalFunctionInfo finfo;

    public FJDivideDescriptor(IExternalFunctionInfo finfo) {
        this.finfo = finfo;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return finfo.getFunctionIdentifier();
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            private ClassLoader classLoader;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {

                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final IPointable inputArg0 = new VoidPointable();
                    private final IPointable inputArg1 = new VoidPointable();
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);

                    private FlexibleJoin<?, ?> flexibleJoin = null;

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        AlgebricksConfig.ALGEBRICKS_LOGGER
                                .info("FJ DIVIDE: ID: " + ctx.getServiceContext().getControllerService().getId());
                        resultStorage.reset();

                        eval0.evaluate(tuple, inputArg0);
                        eval1.evaluate(tuple, inputArg1);

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
                            classLoader = getFlexibleJoinClassLoader((ExternalFJFunctionInfo) finfo, ctx);
                            try {
                                flexibleJoin = getFlexibleJoin((ExternalFJFunctionInfo) finfo, classLoader);
                            } catch (ClassNotFoundException | InvocationTargetException | InstantiationException
                                    | IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }

                        ObjectInput in0;
                        ObjectInput in1;

                        Summary summaryOne;
                        Summary summaryTwo;
                        try {
                            in0 = new ClassLoaderAwareObjectInputStream(dataIn0, classLoader);
                            in1 = new ClassLoaderAwareObjectInputStream(dataIn1, classLoader);
                            summaryOne = (Summary) in0.readObject();
                            summaryTwo = (Summary) in1.readObject();

                        } catch (IOException | ClassNotFoundException e) {
                            throw HyracksDataException.create(e);
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

    @Override
    public IExternalFunctionInfo getFunctionInfo() {
        return finfo;
    }

    @Override
    public IAType[] getArgumentTypes() {
        return new IAType[0];
    }
}
