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
import java.util.Arrays;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.flexiblejoin.SetSimilarityConfig;
import org.apache.asterix.runtime.flexiblejoin.SetSimilarityJoin;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
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

                            ByteArrayInputStream inStream1 =
                                    new ByteArrayInputStream(inputArg1.getByteArray(), offset1 + 1, len1 - 1);
                            DataInputStream dataIn1 = new DataInputStream(inStream1);

                            String key0 = serde.deserialize(dataIn1).getStringValue();

                            ByteArrayInputStream inStream3 =
                                    new ByteArrayInputStream(inputArg3.getByteArray(), offset3 + 1, len3 - 1);
                            DataInputStream dataIn3 = new DataInputStream(inStream3);

                            String key1 = serde.deserialize(dataIn3).getStringValue();

                            SetSimilarityJoin fj = new SetSimilarityJoin(0.5);

                            ByteArrayInputStream inStream4 = new ByteArrayInputStream(bytes4, offset4, len4+1);
                            DataInputStream dataIn4 = new DataInputStream(inStream4);

                            SetSimilarityConfig C = SerializationUtils.deserialize(dataIn4);

                            boolean verifyResult1 = fj.verify(bucketID1, key0, bucketID1, key1, C);

                            if (verifyResult1) {

                                int[] buckets1DA = fj.assign1(key0, C);
                                int[] buckets2DA = fj.assign2(key1, C);

                                Arrays.sort(buckets1DA);
                                Arrays.sort(buckets2DA);

                                boolean stop = false;
                                for (int b1 : buckets1DA) {
                                    for (int b2 : buckets2DA) {
                                        if (fj.match(b1, b2)) {
                                            if (b1 == bucketID0 && b2 == bucketID1) {
                                                verifyResult = true;
                                            }
                                            stop = true;
                                            break;
                                        }
                                    }
                                    if (stop)
                                        break;
                                }
                            }

                        } catch (Exception e) {
                            System.out.println(e.getMessage());
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
