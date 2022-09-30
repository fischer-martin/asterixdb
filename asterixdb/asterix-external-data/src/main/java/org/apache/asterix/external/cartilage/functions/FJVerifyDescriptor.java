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

import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.external.cartilage.base.Configuration;
import org.apache.asterix.external.cartilage.base.FlexibleJoin;
import org.apache.asterix.external.cartilage.util.ClassLoaderAwareObjectInputStream;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.ExternalFJFunctionInfo;
import org.apache.asterix.om.functions.IExternalFunctionDescriptor;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FJVerifyDescriptor extends AbstractScalarFunctionDynamicDescriptor implements IExternalFunctionDescriptor {
    private static final long serialVersionUID = 2L;

    private IAType bucket1Type;
    private IAType key1Type;
    private IAType bucket2Type;
    private IAType key2Type;
    private IAType configType;

    private final IExternalFunctionInfo finfo;

    public FJVerifyDescriptor(IExternalFunctionInfo finfo) {
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

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {

                return new IScalarEvaluator() {

                    private FlexibleJoin flexibleJoin = null;
                    private Configuration configuration = null;

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private PointableAllocator pointableAllocator = new PointableAllocator();
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
                    private final ISerializerDeserializer bserde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);
                    private final EnumDeserializer<ATypeTag> eser = EnumDeserializer.ATYPETAGDESERIALIZER;

                    private ABoolean res;

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();

                        eval0.evaluate(tuple, inputArg0);
                        eval1.evaluate(tuple, inputArg1);
                        eval2.evaluate(tuple, inputArg2);
                        eval3.evaluate(tuple, inputArg3);

                        byte[] bytes0 = inputArg0.getByteArray();

                        byte[] bytes2 = inputArg2.getByteArray();

                        int offset0 = inputArg0.getStartOffset();
                        int offset2 = inputArg2.getStartOffset();

                        try {

                            int bucketID0 = AInt32SerializerDeserializer.getInt(bytes0, offset0 + 1);
                            int bucketID1 = AInt32SerializerDeserializer.getInt(bytes2, offset2 + 1);

                            if (flexibleJoin == null) {
                                AlgebricksConfig.ALGEBRICKS_LOGGER.info(
                                        "FJ VERIFY: ID: " + ctx.getServiceContext().getControllerService().getId());
                                ClassLoader classLoader =
                                        getFlexibleJoinClassLoader((ExternalFJFunctionInfo) finfo, ctx);
                                try {
                                    flexibleJoin = getFlexibleJoin((ExternalFJFunctionInfo) finfo, classLoader);
                                    eval4.evaluate(tuple, inputArg4);
                                    byte[] bytes4 = inputArg4.getByteArray();
                                    int offset4 = inputArg4.getStartOffset();
                                    int len4 = inputArg4.getLength();
                                    ByteArrayInputStream inStream4 =
                                            new ByteArrayInputStream(bytes4, offset4, len4 + 1);
                                    DataInputStream dataIn4 = new DataInputStream(inStream4);
                                    ObjectInput in4 = new ClassLoaderAwareObjectInputStream(dataIn4, classLoader);
                                    configuration = (Configuration) in4.readObject();
                                } catch (ClassNotFoundException | InvocationTargetException | InstantiationException
                                        | IllegalAccessException | IOException e) {
                                    e.printStackTrace();
                                }

                            }

                            Object key1Obj = pointableAllocator.allocateFieldValue(key1Type);
                            eval1.evaluate(tuple, (IVisitablePointable) key1Obj);
                            Object key2Obj = pointableAllocator.allocateFieldValue(key2Type);
                            eval3.evaluate(tuple, (IVisitablePointable) key2Obj);

                            res = flexibleJoin.verify(bucketID0, key1Obj, bucketID1, key2Obj, configuration)
                                    ? ABoolean.TRUE : ABoolean.FALSE;

                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        bserde.serialize(res, resultStorage.getDataOutput());
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
        IAType[] types = { bucket1Type, key1Type, bucket2Type, key2Type, configType };

        return types;
    }

    @Override
    public void setImmutableStates(Object... types) {
        bucket1Type = (IAType) types[0];
        key1Type = (IAType) types[1];
        bucket2Type = (IAType) types[2];
        key2Type = (IAType) types[3];
        configType = (IAType) types[4];
    }

}
