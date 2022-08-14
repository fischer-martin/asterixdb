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
import static org.apache.asterix.external.cartilage.util.ParameterTypeResolver.getKeyObject;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.lang.reflect.InvocationTargetException;

import org.apache.asterix.external.cartilage.base.Configuration;
import org.apache.asterix.external.cartilage.base.FlexibleJoin;
import org.apache.asterix.external.cartilage.util.ClassLoaderAwareObjectInputStream;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.functions.ExternalFJFunctionInfo;
import org.apache.asterix.om.functions.IExternalFunctionDescriptor;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.unnestingfunctions.base.AbstractUnnestingFunctionDynamicDescriptor;
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

public class FJAssignTwoDescriptor extends AbstractUnnestingFunctionDynamicDescriptor
        implements IExternalFunctionDescriptor {
    private static final long serialVersionUID = 1L;

    private IAType keyType;
    private IAType configType;

    private final IExternalFunctionInfo finfo;

    public FJAssignTwoDescriptor(IExternalFunctionInfo finfo) {
        this.finfo = finfo;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return finfo.getFunctionIdentifier();
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
                    private PointableAllocator pointableAllocator = new PointableAllocator();
                    private final IPointable inputArg0 = new VoidPointable();
                    private final IPointable inputArg1 = new VoidPointable();
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);
                    int pos;

                    private final AMutableInt32 aInt32 = new AMutableInt32(0);

                    @SuppressWarnings("rawtypes")
                    private final ISerializerDeserializer serde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);

                    private FlexibleJoin flexibleJoin = null;
                    private Configuration configuration = null;

                    @Override
                    public void init(IFrameTupleReference tuple) throws HyracksDataException {
                        eval0.evaluate(tuple, inputArg0);
                        byte[] bytes0 = inputArg0.getByteArray();
                        int offset0 = inputArg0.getStartOffset();
                        int len = inputArg0.getLength();

                        ATypeTag tag0 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]);

                        if (flexibleJoin == null) {
                            AlgebricksConfig.ALGEBRICKS_LOGGER.info(
                                    "FJ ASSIGN TWO: ID: " + ctx.getServiceContext().getControllerService().getId());
                            ClassLoader classLoader = getFlexibleJoinClassLoader((ExternalFJFunctionInfo) finfo, ctx);
                            try {
                                flexibleJoin = getFlexibleJoin((ExternalFJFunctionInfo) finfo, classLoader);
                                eval1.evaluate(tuple, inputArg1);
                                byte[] bytes1 = inputArg1.getByteArray();
                                int offset1 = inputArg1.getStartOffset();
                                int len1 = inputArg1.getLength();
                                ByteArrayInputStream inStream1 = new ByteArrayInputStream(bytes1, offset1, len1 + 1);
                                DataInputStream dataIn1 = new DataInputStream(inStream1);
                                ObjectInput in1 = new ClassLoaderAwareObjectInputStream(dataIn1, classLoader);
                                configuration = (Configuration) in1.readObject();
                            } catch (ClassNotFoundException | InvocationTargetException | InstantiationException
                                    | IllegalAccessException | IOException e) {
                                e.printStackTrace();
                            }
                        }

                        ByteArrayInputStream inStream =
                                new ByteArrayInputStream(inputArg0.getByteArray(), offset0 + 1, len - 1);
                        DataInputStream dataIn = new DataInputStream(inStream);
                        if (tag0 == ATypeTag.OBJECT) {
                            IVisitablePointable obj = pointableAllocator.allocateFieldValue(keyType);
                            eval0.evaluate(tuple, obj);
                            // I suppose that this is a typo and we should actually call assign2() instead of assign1()
                            //buckets = flexibleJoin.assign1(obj, configuration);
                            buckets = flexibleJoin.assign2(obj, configuration);
                        } else {
                            // I suppose that this is a typo and we should actually call assign2() instead of assign1()
                            //buckets = flexibleJoin.assign1(getKeyObject(dataIn, tag0), configuration);
                            buckets = flexibleJoin.assign2(getKeyObject(dataIn, tag0), configuration);
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

    @Override
    public IExternalFunctionInfo getFunctionInfo() {
        return null;
    }

    @Override
    public IAType[] getArgumentTypes() {
        IAType[] types = {keyType, configType};

        return types;
    }

    @Override
    public void setImmutableStates(Object... types) {
        keyType = (IAType) types[0];
        configType = (IAType) types[1];
    }
}
