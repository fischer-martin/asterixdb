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

import org.apache.asterix.external.cartilage.base.FlexibleJoin;
import org.apache.asterix.external.cartilage.base.Summary;
import org.apache.asterix.external.cartilage.util.ClassLoaderAwareObjectInputStream;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.functions.ExternalFJFunctionInfo;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.aggregates.std.AbstractAggregateFunction;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.commons.lang3.SerializationUtils;
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

    protected ATypeTag typeTag;
    private IExternalFunctionInfo finfo;

    private ClassLoader classLoader;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ANull> nullSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);

    public AbstractSummaryOneAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc, IExternalFunctionInfo finfo) throws HyracksDataException {
        super(sourceLoc);
        this.eval = args[0].createScalarEvaluator(context);
        this.context = context;
        this.finfo = finfo;

        classLoader = getFlexibleJoinClassLoader((ExternalFJFunctionInfo) finfo, context);
        try {
            FlexibleJoin<?, ?> flexibleJoin = getFlexibleJoin((ExternalFJFunctionInfo) finfo, classLoader);
            this.summary = flexibleJoin.createSummarizer1();
        } catch (ClassNotFoundException | InvocationTargetException | InstantiationException
                | IllegalAccessException e) {
            e.printStackTrace();
        }
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

        typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);

        if (typeTag == ATypeTag.NULL || typeTag == ATypeTag.MISSING) {
            processNull(typeTag);
        } else {
            ByteArrayInputStream inStream = new ByteArrayInputStream(data, offset + 1, len - 1);
            DataInputStream dataIn = new DataInputStream(inStream);
            summary.add(getKeyObject(dataIn, typeTag));
        }
    }

    public void processPartialResults(IFrameTupleReference tuple) throws IOException {
        eval.evaluate(tuple, inputVal);
        byte[] data = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();
        int len = inputVal.getLength();

        try {
            ByteArrayInputStream inStream = new ByteArrayInputStream(data, offset, len + 1);
            DataInputStream dataIn = new DataInputStream(inStream);
            ObjectInput in = new ClassLoaderAwareObjectInputStream(dataIn, classLoader);
            Summary<?> summaryTemp = (Summary<?>) in.readObject();
            summary.add(summaryTemp);
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
        throw new UnsupportedItemTypeException(sourceLoc, finfo.getFunctionIdentifier(), typeTag.serialize());
    }
}
