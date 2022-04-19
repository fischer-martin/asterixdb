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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.functions.FunctionDescriptorTag;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.external.cartilage.base.Configuration;
import org.apache.asterix.external.cartilage.base.FlexibleJoin;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.asterix.external.library.JavaLibrary;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.functions.IExternalFJFunctionInfo;
import org.apache.asterix.om.functions.IExternalFunctionDescriptor;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FJMatchDescriptor extends AbstractScalarFunctionDynamicDescriptor implements IExternalFunctionDescriptor {

    private static final long serialVersionUID = 2L;
    private final IExternalFunctionInfo finfo;
    private IAType[] argTypes;

    public FJMatchDescriptor(IExternalFunctionInfo finfo) {
        this.finfo = finfo;
    }

    @Override
    public void setImmutableStates(Object... states) {

    }

    @Override
    public void setSourceLocation(SourceLocation sourceLoc) {

    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return finfo.getFunctionIdentifier();
    }

    @Override
    public FunctionDescriptorTag getFunctionDescriptorTag() {
        return null;
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
                            DataverseName libraryDataverseName = finfo.getLibraryDataverseName();
                            String libraryName = finfo.getLibraryName();
                            ExternalLibraryManager libraryManager =
                                    (ExternalLibraryManager) ((INcApplicationContext) ctx.getServiceContext()
                                            .getApplicationContext()).getLibraryManager();
                            JavaLibrary library =
                                    (JavaLibrary) libraryManager.getLibrary(libraryDataverseName, libraryName);

                            String classname = finfo.getExternalIdentifier().get(0);
                            flexibleJoinClass = Class.forName(classname, false, library.getClassLoader());
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    }

                    private FlexibleJoin flexibleJoin = null;
                    private Configuration configuration = null;
                    private List<IAObject> parameters = null;

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final IPointable inputArg0 = new VoidPointable();
                    private final IPointable inputArg1 = new VoidPointable();
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();

                        eval0.evaluate(tuple, inputArg0);
                        eval1.evaluate(tuple, inputArg1);

                        if (flexibleJoin == null) {
                            AlgebricksConfig.ALGEBRICKS_LOGGER
                                    .info("FJ MATCH: ID: " + ctx.getServiceContext().getControllerService().getId());
                            Constructor<?> flexibleJoinConstructor = flexibleJoinClass.getConstructors()[0];
                            List<Object> parametersList = new ArrayList<>();
                            parameters = ((IExternalFJFunctionInfo) finfo).getParameters();
                            if (!parameters.isEmpty()) {
                                for (IAObject p : parameters) {
                                    switch (p.getType().getTypeTag()) {
                                        case DOUBLE:
                                            parametersList.add(((ADouble) p).getDoubleValue());
                                            break;
                                        case BIGINT:
                                            parametersList.add(((AInt64) p).getLongValue());
                                            break;
                                        case INTEGER:
                                            parametersList.add(((AInt32) p).getIntegerValue());
                                            break;
                                    }
                                }
                                try {
                                    flexibleJoin =
                                            (FlexibleJoin) flexibleJoinConstructor.newInstance(parametersList.get(0));
                                } catch (InstantiationException e) {
                                    e.printStackTrace();
                                } catch (IllegalAccessException e) {
                                    e.printStackTrace();
                                } catch (InvocationTargetException e) {
                                    e.printStackTrace();
                                }
                            } else {
                                try {
                                    flexibleJoin = (FlexibleJoin) flexibleJoinConstructor.newInstance();
                                } catch (InstantiationException e) {
                                    e.printStackTrace();
                                } catch (IllegalAccessException e) {
                                    e.printStackTrace();
                                } catch (InvocationTargetException e) {
                                    e.printStackTrace();
                                }
                            }

                        }

                        //if (PointableHelper.checkAndSetMissingOrNull(result, inputArg0, inputArg1)) {
                        //    return;
                        //}

                        byte[] bytes0 = inputArg0.getByteArray();
                        byte[] bytes1 = inputArg1.getByteArray();

                        int offset0 = inputArg0.getStartOffset();
                        int offset1 = inputArg1.getStartOffset();

                        int bucketID0 = AInt32SerializerDeserializer.getInt(bytes0, offset0 + 1);
                        int bucketID1 = AInt32SerializerDeserializer.getInt(bytes1, offset1 + 1);

                        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isDebugEnabled()) {
                            AlgebricksConfig.ALGEBRICKS_LOGGER
                                    .info("\nFJ MATCH: ID: " + ctx.getServiceContext().getControllerService().getId()
                                            + " bucket ids: " + bucketID0 + ", " + bucketID1);
                        }

                        boolean matchResult = flexibleJoin.match(bucketID0, bucketID1);

                        try {
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN)
                                    .serialize(matchResult ? ABoolean.TRUE : ABoolean.FALSE,
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

    @Override
    public IExternalFunctionInfo getFunctionInfo() {
        return finfo;
    }

    @Override
    public IAType[] getArgumentTypes() {
        return new IAType[0];
    }
}
