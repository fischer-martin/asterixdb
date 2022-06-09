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
package org.apache.asterix.runtime.operators.joins.flexible;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.join.NestedLoopJoin;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlexibleJoinOperatorDescriptor extends AbstractOperatorDescriptor {

    /**
     * Use a random seed to avoid hash collision with the hash exchange operator.
     * See https://issues.apache.org/jira/browse/ASTERIXDB-2783 for more details.
     */
    private static final int INIT_SEED = 982028031;

    private static final double NLJ_SWITCH_THRESHOLD = 0.8;

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger();

    private static final int JOIN_BUILD_ACTIVITY_ID = 0;
    private static final int JOIN_PROBE_ACTIVITY_ID = 1;
    private final int[] buildKeys;
    private final int[] probeKeys;
    private final int memoryForJoin;

    private static final String PROBE_REL = "RelR";
    private static final String BUILD_REL = "RelS";

    private final IPredicateEvaluatorFactory predEvaluatorFactory;

    private final IBinaryHashFunctionFamily[] propHashFunctionFactories;
    private final IBinaryHashFunctionFamily[] buildHashFunctionFactories;

    private final ITuplePairComparatorFactory tuplePairComparatorFactoryProbe2Build; //For HHJ & NLJ in probe
    private final ITuplePairComparatorFactory tuplePairComparatorFactoryBuild2Probe; //For HHJ & NLJ in probe

    private final double fudgeFactor;

    public FlexibleJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memoryForJoin, int[] buildKeys,
            int[] probeKeys, RecordDescriptor recordDescriptor,
            ITuplePairComparatorFactory tupPaircomparatorFactory01,ITuplePairComparatorFactory tupPaircomparatorFactory10,
                                          IBinaryHashFunctionFamily[] propHashFunctionFactories,
                                          IBinaryHashFunctionFamily[] buildHashFunctionFactories,
                                          IPredicateEvaluatorFactory predEvaluatorFactory, double fudgeFactor) {
        super(spec, 2, 1);

        this.predEvaluatorFactory = predEvaluatorFactory;
        outRecDescs[0] = recordDescriptor;
        this.buildKeys = buildKeys;
        this.probeKeys = probeKeys;
        this.memoryForJoin = memoryForJoin;

        this.propHashFunctionFactories = propHashFunctionFactories;
        this.buildHashFunctionFactories = buildHashFunctionFactories;

        this.tuplePairComparatorFactoryProbe2Build = tupPaircomparatorFactory01;
        this.tuplePairComparatorFactoryBuild2Probe = tupPaircomparatorFactory10;

        this.fudgeFactor = fudgeFactor;
    }

    //memorySize is the memory for join (we have already excluded the 2 buffers for in/out)
    private static int getNumberOfPartitions(int memorySize, int buildSize, double factor, int nPartitions)
            throws HyracksDataException {
        if (memorySize <= 2) {
            throw new HyracksDataException("Not enough memory is available for Hybrid Hash Join.");
        }
        int minimumNumberOfPartitions = Math.min(20, memorySize);
        if (buildSize < 0 || memorySize > buildSize * factor) {

            return minimumNumberOfPartitions;
        }
        // Two frames are already excluded from the memorySize for taking the input and output into account. That
        // makes the denominator in the following formula to be different than the denominator in original Hybrid Hash
        // Join which is memorySize - 1. This formula gives the total number of partitions, the spilled partitions
        // and the memory-resident partition ( + 1 in formula is for taking the memory-resident partition into account).
        int numberOfPartitions = (int) (Math.ceil((buildSize * factor / nPartitions - memorySize) / (memorySize))) + 1;
        numberOfPartitions = Math.max(minimumNumberOfPartitions, numberOfPartitions);
        if (numberOfPartitions > memorySize) { // Considers applying Grace Hash Join instead of Hybrid Hash Join.
            numberOfPartitions = (int) Math.ceil(Math.sqrt(buildSize * factor / nPartitions));
            return Math.max(2, Math.min(numberOfPartitions, memorySize));
        }
        return numberOfPartitions;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId buildAid = new ActivityId(odId, JOIN_BUILD_ACTIVITY_ID);
        ActivityId probeAid = new ActivityId(odId, JOIN_PROBE_ACTIVITY_ID);

        IActivity probeAN = new JoinProbeActivityNode(probeAid);
        IActivity buildAN = new JoinBuildActivityNode(buildAid, probeAid);

        builder.addActivity(this, buildAN);
        builder.addSourceEdge(0, buildAN, 0);

        builder.addActivity(this, probeAN);
        builder.addSourceEdge(1, probeAN, 0);
        builder.addTargetEdge(0, probeAN, 0);
        builder.addBlockingEdge(buildAN, probeAN);
    }

    public static class JoinCacheTaskState extends AbstractStateObject {
        private OptimizedFlexibleJoiner joiner;

        private int memForJoin;
        private int numOfPartitions;

        private JoinCacheTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }
    }

    private class JoinBuildActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId nljAid;

        public JoinBuildActivityNode(ActivityId id, ActivityId nljAid) {
            super(id);
            this.nljAid = nljAid;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            final RecordDescriptor probeRd = recordDescProvider.getInputRecordDescriptor(nljAid, 0);
            final RecordDescriptor buildRd = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);

            final IPredicateEvaluator predEvaluator =
                    (predEvaluatorFactory == null ? null : predEvaluatorFactory.createPredicateEvaluator());

            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private JoinCacheTaskState state;

                final ITuplePartitionComputer probeHpc =
                        new FieldHashPartitionComputerFamily(probeKeys, propHashFunctionFactories)
                                .createPartitioner(INIT_SEED);
                final ITuplePartitionComputer buildHpc =
                        new FieldHashPartitionComputerFamily(buildKeys, buildHashFunctionFactories)
                                .createPartitioner(INIT_SEED);

                boolean failed = false;

                @Override
                public void open() throws HyracksDataException {
                    if (memoryForJoin <= 2) { //Dedicated buffers: One buffer to read and two buffers for output
                        throw new HyracksDataException("Not enough memory is assigend for Flexible Join.");
                    }

                    state = new JoinCacheTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));

                    state.memForJoin = memoryForJoin - 2;
                    state.numOfPartitions = getNumberOfPartitions(state.memForJoin, -1, fudgeFactor, nPartitions);

                    state.joiner = new OptimizedFlexibleJoiner(ctx, memoryForJoin, buildRd, probeRd, partition, probeHpc, buildHpc, BUILD_REL, PROBE_REL, predEvaluator);

                    state.joiner.initBuild();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.joiner.build(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    if (state.joiner != null) {
                        if (!failed) {
                            state.joiner.closeBuild();
                            ctx.setStateObject(state);
                            if (LOGGER.isTraceEnabled()) {
                                LOGGER.trace("Flexible Join closed its build phase");
                            }
                        } else {
                            state.joiner.clearBuildTempFiles();
                        }
                    }
//                    state.joiner.processBuildClose();
//                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                    failed = true;
                    if (state.joiner != null) {
                        state.joiner.fail();
                    }
                }
            };
        }
    }

    private class JoinProbeActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId buildAid;

        public JoinProbeActivityNode(ActivityId id) {
            super(id);
            this.buildAid = id;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) throws HyracksDataException {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private JoinCacheTaskState state;
                boolean failed = false;
                final ITuplePairComparator probComp = tuplePairComparatorFactoryProbe2Build.createTuplePairComparator(ctx);
                final ITuplePairComparator buildComp = tuplePairComparatorFactoryBuild2Probe.createTuplePairComparator(ctx);

                final IPredicateEvaluator predEvaluator =
                        predEvaluatorFactory == null ? null : predEvaluatorFactory.createPredicateEvaluator();

                final RecordDescriptor buildRd = recordDescProvider.getInputRecordDescriptor(buildAid, 0);
                final RecordDescriptor probeRd = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);

                private IFrame rPartbuff = new VSizeFrame(ctx);
                @Override
                public void open() throws HyracksDataException {
                    writer.open();
                    state = (JoinCacheTaskState) ctx.getStateObject(
                            new TaskId(new ActivityId(getOperatorId(), JOIN_BUILD_ACTIVITY_ID), partition));

                    state.joiner.initProbe(probComp);

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Flexible Join is starting the probe phase.");
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.joiner.probe(buffer, writer);
                }

                @Override
                public void close() throws HyracksDataException {
                    if (failed) {
                        try {
                            // Clear temp files if fail() was called.
                            state.joiner.clearBuildTempFiles();
                            state.joiner.clearProbeTempFiles();
                        } finally {
                            writer.close(); // writer should always be closed.
                        }
                        logProbeComplete();
                        return;
                    }
                    try {
                        try {
                            state.joiner.completeProbe(writer);
                        } finally {
                            state.joiner.releaseResource();
                        }
                        BitSet partitionStatus = state.joiner.getPartitionStatus();
                        rPartbuff.reset();
                        for (int pid = partitionStatus.nextSetBit(0); pid >= 0; pid =
                                partitionStatus.nextSetBit(pid + 1)) {
                            RunFileReader bReader = state.joiner.getBuildRFReader(pid);
                            RunFileReader pReader = state.joiner.getProbeRFReader(pid);

                            if (bReader == null || pReader == null) {
//                                if (isLeftOuter && pReader != null) {
//                                    appendNullToProbeTuples(pReader);
//                                }
                                if (bReader != null) {
                                    bReader.close();
                                }
                                if (pReader != null) {
                                    pReader.close();
                                }
                                continue;
                            }
                            int bSize = state.joiner.getBuildPartitionSizeInTup(pid);
                            int pSize = state.joiner.getProbePartitionSizeInTup(pid);
                            joinPartitionPair(bReader, pReader, bSize, pSize, 1);
                        }
                    } catch (Exception e) {
                        if (state.joiner != null) {
                            state.joiner.fail();
                        }
                        // Since writer.nextFrame() is called in the above "try" body, we have to call writer.fail()
                        // to send the failure signal to the downstream, when there is a throwable thrown.
                        writer.fail();
                        // Clear temp files as this.fail() nor this.close() will no longer be called after close().
                        state.joiner.clearBuildTempFiles();
                        state.joiner.clearProbeTempFiles();
                        // Re-throw the whatever is caught.
                        throw e;
                    } finally {
                        try {
                            logProbeComplete();
                        } finally {
                            writer.close();
                        }
                    }
                }

                private void logProbeComplete() {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Flexible Join closed its probe phase");
                    }
                }

                //The buildSideReader should be always the original buildSideReader, so should the probeSideReader
                private void joinPartitionPair(RunFileReader buildSideReader, RunFileReader probeSideReader,
                                               int buildSizeInTuple, int probeSizeInTuple, int level) throws HyracksDataException {
                    ITuplePartitionComputer probeHpc =
                            new FieldHashPartitionComputerFamily(probeKeys, propHashFunctionFactories)
                                    .createPartitioner(level);
                    ITuplePartitionComputer buildHpc =
                            new FieldHashPartitionComputerFamily(buildKeys, buildHashFunctionFactories)
                                    .createPartitioner(level);

                    int frameSize = ctx.getInitialFrameSize();
                    long buildPartSize = (long) Math.ceil((double) buildSideReader.getFileSize() / (double) frameSize);
                    long probePartSize = (long) Math.ceil((double) probeSideReader.getFileSize() / (double) frameSize);
                    int beforeMax = Math.max(buildSizeInTuple, probeSizeInTuple);

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("\n>>>Joining Partition Pairs (thread_id " + Thread.currentThread().getId()
                                + ") (pid " + ") - (level " + level + ")" + " - BuildSize:\t" + buildPartSize
                                + "\tProbeSize:\t" + probePartSize + " - MemForJoin " + (state.memForJoin));
                    }

                    // Calculate the expected hash table size for the both side.
                    long expectedHashTableSizeForBuildInFrame =
                            SerializableHashTable.getExpectedTableFrameCount(buildSizeInTuple, frameSize);
                    long expectedHashTableSizeForProbeInFrame =
                            SerializableHashTable.getExpectedTableFrameCount(probeSizeInTuple, frameSize);

                    //Apply in-Mem HJ if possible
                    if ((buildPartSize + expectedHashTableSizeForBuildInFrame < state.memForJoin)
                            || (probePartSize + expectedHashTableSizeForProbeInFrame < state.memForJoin)) {

                        int tabSize = -1;
                        if (buildPartSize < probePartSize) {
                            //Case 1.1 - InMemHJ (without Role-Reversal)
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("\t>>>Case 1.1 (IsLeftOuter || buildSize<probe) AND ApplyInMemHJ - [Level "
                                        + level + "]");
                            }
                            tabSize = buildSizeInTuple;
                            if (tabSize == 0) {
                                throw new HyracksDataException(
                                        "Trying to join an empty partition. Invalid table size for inMemoryHashJoin.");
                            }
                            //Build Side is smaller
                            applyInMemHashJoin(buildKeys, probeKeys, tabSize, buildRd, probeRd, buildHpc, probeHpc,
                                    buildSideReader, probeSideReader, probComp); // checked-confirmed
                        } else { //Case 1.2 - InMemHJ with Role Reversal
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("\t>>>Case 1.2. (NoIsLeftOuter || probe<build) AND ApplyInMemHJ"
                                        + "WITH RoleReversal - [Level " + level + "]");
                            }
                            tabSize = probeSizeInTuple;
                            if (tabSize == 0) {
                                throw new HyracksDataException(
                                        "Trying to join an empty partition. Invalid table size for inMemoryHashJoin.");
                            }
                            //Probe Side is smaller
                            applyInMemHashJoin(probeKeys, buildKeys, tabSize, probeRd, buildRd, probeHpc, buildHpc,
                                    probeSideReader, buildSideReader, buildComp); // checked-confirmed
                        }
                    }
                    //Apply (Recursive) HHJ
                    else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("\t>>>Case 2. ApplyRecursiveHHJ - [Level " + level + "]");
                        }
                        if (buildPartSize < probePartSize) {
                            //Case 2.1 - Recursive HHJ (without Role-Reversal)
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(
                                        "\t\t>>>Case 2.1 - RecursiveHHJ WITH (isLeftOuter || build<probe) - [Level "
                                                + level + "]");
                            }
                            applyHybridHashJoin((int) buildPartSize, PROBE_REL, BUILD_REL, probeKeys, buildKeys,
                                    probeRd, buildRd, probeHpc, buildHpc, probeSideReader, buildSideReader, level,
                                    beforeMax, probComp);

                        } else { //Case 2.2 - Recursive HHJ (with Role-Reversal)
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(
                                        "\t\t>>>Case 2.2. - RecursiveHHJ WITH RoleReversal - [Level " + level + "]");
                            }

                            applyHybridHashJoin((int) probePartSize, BUILD_REL, PROBE_REL, buildKeys, probeKeys,
                                    buildRd, probeRd, buildHpc, probeHpc, buildSideReader, probeSideReader, level,
                                    beforeMax, buildComp);

                        }
                    }
                }

                private void applyHybridHashJoin(int tableSize, final String PROBE_REL, final String BUILD_REL,
                                                 final int[] probeKeys, final int[] buildKeys, final RecordDescriptor probeRd,
                                                 final RecordDescriptor buildRd, final ITuplePartitionComputer probeHpc,
                                                 final ITuplePartitionComputer buildHpc, RunFileReader probeSideReader,
                                                 RunFileReader buildSideReader, final int level, final long beforeMax, ITuplePairComparator comp)
                        throws HyracksDataException {

                    boolean isReversed = probeKeys == FlexibleJoinOperatorDescriptor.this.buildKeys
                            && buildKeys == FlexibleJoinOperatorDescriptor.this.probeKeys;

                    OptimizedFlexibleJoiner recursiveFlexibleJoin;
                    int n = getNumberOfPartitions(state.memForJoin, tableSize, fudgeFactor, nPartitions);
                    recursiveFlexibleJoin = new OptimizedFlexibleJoiner(ctx, state.memForJoin, buildRd, probeRd, n, probeHpc, buildHpc, PROBE_REL, BUILD_REL, predEvaluator); //checked-confirmed

                    recursiveFlexibleJoin.setIsReversed(isReversed);
                    try {
                        buildSideReader.open();
                        try {
                            recursiveFlexibleJoin.initBuild();
                            rPartbuff.reset();
                            while (buildSideReader.nextFrame(rPartbuff)) {
                                recursiveFlexibleJoin.build(rPartbuff.getBuffer());
                            }
                        } finally {
                            // Makes sure that files are always properly closed.
                            recursiveFlexibleJoin.closeBuild();
                        }
                    } finally {
                        buildSideReader.close();
                    }
                    try {
                        probeSideReader.open();
                        rPartbuff.reset();
                        try {
                            recursiveFlexibleJoin.initProbe(comp);
                            while (probeSideReader.nextFrame(rPartbuff)) {
                                recursiveFlexibleJoin.probe(rPartbuff.getBuffer(), writer);
                            }
                            recursiveFlexibleJoin.completeProbe(writer);
                        } finally {
                            recursiveFlexibleJoin.releaseResource();
                        }
                    } finally {
                        // Makes sure that files are always properly closed.
                        probeSideReader.close();
                    }

                    try {
                        int maxAfterBuildSize = recursiveFlexibleJoin.getMaxBuildPartitionSize();
                        int maxAfterProbeSize = recursiveFlexibleJoin.getMaxProbePartitionSize();
                        int afterMax = Math.max(maxAfterBuildSize, maxAfterProbeSize);

                        BitSet rPStatus = recursiveFlexibleJoin.getPartitionStatus();
                        if ((afterMax < (NLJ_SWITCH_THRESHOLD * beforeMax))) {
                            //Case 2.1.1 - Keep applying HHJ
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("\t\t>>>Case 2.1.1 - KEEP APPLYING RecursiveHHJ WITH "
                                        + "(isLeftOuter || build<probe) - [Level " + level + "]");
                            }
                            for (int rPid = rPStatus.nextSetBit(0); rPid >= 0; rPid = rPStatus.nextSetBit(rPid + 1)) {
                                RunFileReader rbrfw = recursiveFlexibleJoin.getBuildRFReader(rPid);
                                RunFileReader rprfw = recursiveFlexibleJoin.getProbeRFReader(rPid);
                                int rbSizeInTuple = recursiveFlexibleJoin.getBuildPartitionSizeInTup(rPid);
                                int rpSizeInTuple = recursiveFlexibleJoin.getProbePartitionSizeInTup(rPid);

                                if (rbrfw == null || rprfw == null) {
                                    if (rbrfw != null) {
                                        rbrfw.close();
                                    }
                                    if (rprfw != null) {
                                        rprfw.close();
                                    }
                                    continue;
                                }

                                if (isReversed) {
                                    joinPartitionPair(rprfw, rbrfw, rpSizeInTuple, rbSizeInTuple, level + 1);
                                } else {
                                    joinPartitionPair(rbrfw, rprfw, rbSizeInTuple, rpSizeInTuple, level + 1);
                                }
                            }

                        } else { //Case 2.1.2 - Switch to NLJ
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("\t\t>>>Case 2.1.2 - SWITCHED to NLJ RecursiveHHJ WITH "
                                        + "(isLeftOuter || build<probe) - [Level " + level + "]");
                            }
                            for (int rPid = rPStatus.nextSetBit(0); rPid >= 0; rPid = rPStatus.nextSetBit(rPid + 1)) {
                                RunFileReader rbrfw = recursiveFlexibleJoin.getBuildRFReader(rPid);
                                RunFileReader rprfw = recursiveFlexibleJoin.getProbeRFReader(rPid);

                                if (rbrfw == null || rprfw == null) {
                                    if (rbrfw != null) {
                                        rbrfw.close();
                                    }
                                    if (rprfw != null) {
                                        rprfw.close();
                                    }
                                    continue;
                                }

                                int buildSideInTups = recursiveFlexibleJoin.getBuildPartitionSizeInTup(rPid);
                                int probeSideInTups = recursiveFlexibleJoin.getProbePartitionSizeInTup(rPid);
                                // NLJ order is outer + inner, the order is reversed from the other joins
                                if (probeSideInTups < buildSideInTups) {
                                    //checked-modified
                                    applyNestedLoopJoin(probeRd, buildRd, memoryForJoin, rprfw, rbrfw);
                                } else {
                                    //checked-modified
                                    applyNestedLoopJoin(buildRd, probeRd, memoryForJoin, rbrfw, rprfw);
                                }
                            }
                        }
                    } catch (Exception e) {
                        // Make sure that temporary run files generated in recursive hybrid hash joins
                        // are closed and deleted.
                        recursiveFlexibleJoin.clearBuildTempFiles();
                        recursiveFlexibleJoin.clearProbeTempFiles();
                        throw e;
                    }
                }

                private void applyNestedLoopJoin(RecordDescriptor outerRd, RecordDescriptor innerRd, int memorySize,
                                                 RunFileReader outerReader, RunFileReader innerReader) throws HyracksDataException {
                    // The nested loop join result is outer + inner. All the other operator is probe + build.
                    // Hence the reverse relation is different.
                    boolean isReversed = outerRd == buildRd && innerRd == probeRd;
                    ITuplePairComparator nljComptorOuterInner = isReversed ? buildComp : probComp;
                    NestedLoopJoin nlj = new NestedLoopJoin(ctx.getJobletContext(), new FrameTupleAccessor(outerRd),
                            new FrameTupleAccessor(innerRd), memorySize, predEvaluator, false, null,
                            isReversed);
                    nlj.setComparator(nljComptorOuterInner);

                    IFrame cacheBuff = new VSizeFrame(ctx);
                    try {
                        innerReader.open();
                        while (innerReader.nextFrame(cacheBuff)) {
                            nlj.cache(cacheBuff.getBuffer());
                            cacheBuff.reset();
                        }
                    } finally {
                        try {
                            nlj.closeCache();
                        } finally {
                            innerReader.close();
                        }
                    }
                    try {
                        IFrame joinBuff = new VSizeFrame(ctx);
                        outerReader.open();
                        try {
                            while (outerReader.nextFrame(joinBuff)) {
                                nlj.join(joinBuff.getBuffer(), writer);
                                joinBuff.reset();
                            }
                            nlj.completeJoin(writer);
                        } finally {
                            nlj.releaseMemory();
                        }
                    } finally {
                        outerReader.close();
                    }
                }

                private void applyInMemHashJoin(int[] bKeys, int[] pKeys, int tabSize, RecordDescriptor buildRDesc,
                                                RecordDescriptor probeRDesc, ITuplePartitionComputer hpcRepBuild,
                                                ITuplePartitionComputer hpcRepProbe, RunFileReader bReader, RunFileReader pReader,
                                                ITuplePairComparator comp) throws HyracksDataException {
                    boolean isReversed = pKeys == FlexibleJoinOperatorDescriptor.this.buildKeys
                            && bKeys == FlexibleJoinOperatorDescriptor.this.probeKeys;
                    IDeallocatableFramePool framePool =
                            new DeallocatableFramePool(ctx, state.memForJoin * ctx.getInitialFrameSize());
                    ISimpleFrameBufferManager bufferManager = new FramePoolBackedFrameBufferManager(framePool);

                    ISerializableTable table = new SerializableHashTable(tabSize, ctx, bufferManager);
                    InMemoryFlexibleJoin joiner = new InMemoryFlexibleJoin(ctx, new FrameTupleAccessor(probeRDesc),
                            hpcRepProbe, new FrameTupleAccessor(buildRDesc), buildRDesc, hpcRepBuild,
                            table, predEvaluator, isReversed, bufferManager);
                    joiner.setComparator(comp);
                    try {
                        bReader.open();
                        rPartbuff.reset();
                        while (bReader.nextFrame(rPartbuff)) {
                            // We need to allocate a copyBuffer, because this buffer gets added to the buffers list
                            // in the InMemoryHashJoin.
                            ByteBuffer copyBuffer = bufferManager.acquireFrame(rPartbuff.getFrameSize());
                            // If a frame cannot be allocated, there may be a chance if we can compact the table,
                            // one or more frame may be reclaimed.
                            if (copyBuffer == null) {
                                if (joiner.compactHashTable() > 0) {
                                    copyBuffer = bufferManager.acquireFrame(rPartbuff.getFrameSize());
                                }
                                if (copyBuffer == null) {
                                    // Still no frame is allocated? At this point, we have no way to get a frame.
                                    throw new HyracksDataException(
                                            "Can't allocate one more frame. Assign more memory to InMemoryHashJoin.");
                                }
                            }
                            FrameUtils.copyAndFlip(rPartbuff.getBuffer(), copyBuffer);
                            joiner.build(copyBuffer);
                            rPartbuff.reset();
                        }
                    } finally {
                        bReader.close();
                    }
                    try {
                        //probe
                        pReader.open();
                        rPartbuff.reset();
                        try {
                            while (pReader.nextFrame(rPartbuff)) {
                                joiner.join(rPartbuff.getBuffer(), writer);
                                rPartbuff.reset();
                            }
                            joiner.completeJoin(writer);
                        } finally {
                            joiner.releaseMemory();
                        }
                    } finally {
                        try {
                            pReader.close();
                        } finally {
                            joiner.closeTable();
                        }
                    }
                }


                @Override
                public void fail() throws HyracksDataException {
                    failed = true;
                    if (state.joiner != null) {
                        state.joiner.fail();
                    }
                    writer.fail();
                }
            };
        }
    }
}
