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

import org.apache.asterix.runtime.operators.joins.flexible.utils.Bucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IHeuristicForThetaJoin;
import org.apache.asterix.runtime.operators.joins.flexible.utils.heuristics.BigFirst;
import org.apache.asterix.runtime.operators.joins.flexible.utils.heuristics.FirstFit;
import org.apache.asterix.runtime.operators.joins.flexible.utils.heuristics.SmallFirst;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFileStream;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Scanner;

public class ThetaFlexibleJoinOperatorDescriptorConseqH extends AbstractOperatorDescriptor {

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

    private final ITuplePairComparatorFactory tuplePairComparatorFactoryProbe2Build;
    private final ITuplePairComparatorFactory tuplePairComparatorFactoryBuild2Probe;

    private final double fudgeFactor;
    private final String heuristic;

    public ThetaFlexibleJoinOperatorDescriptorConseqH(IOperatorDescriptorRegistry spec, int memoryForJoin, int[] buildKeys,
                                                      int[] probeKeys, RecordDescriptor recordDescriptor, ITuplePairComparatorFactory tupPaircomparatorFactory01,
                                                      ITuplePairComparatorFactory tupPaircomparatorFactory10, IPredicateEvaluatorFactory predEvaluatorFactory,
                                                      double fudgeFactor, String heuristic) {
        super(spec, 2, 1);

        this.predEvaluatorFactory = predEvaluatorFactory;
        outRecDescs[0] = recordDescriptor;
        this.buildKeys = buildKeys;
        this.probeKeys = probeKeys;
        this.memoryForJoin = memoryForJoin;

        this.tuplePairComparatorFactoryProbe2Build = tupPaircomparatorFactory01;
        this.tuplePairComparatorFactoryBuild2Probe = tupPaircomparatorFactory10;

        this.fudgeFactor = fudgeFactor;

        this.heuristic = heuristic;
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
        private ThetaFlexibleJoiner joiner;

        private int memForJoin;

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

                boolean failed = false;

                @Override
                public void open() throws HyracksDataException {
                    if (memoryForJoin <= 2) { //Dedicated buffers: One buffer to read and two buffers for output
                        throw new HyracksDataException("Not enough memory is assigend for Flexible Join.");
                    }

                    state = new JoinCacheTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));

                    LOGGER.info("Theta Join Starts");

                    state.memForJoin = memoryForJoin - 2;

                    state.joiner = new ThetaFlexibleJoiner(ctx, memoryForJoin, buildRd, probeRd, PROBE_REL, BUILD_REL,
                            buildKeys, probeKeys, predEvaluator);

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
                            state.joiner.clearBuild();
                        }
                    }
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                    failed = true;
                    if (state.joiner != null) {
                        state.joiner.clearBuild();
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
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private JoinCacheTaskState state;
                boolean failed = false;
                final ITuplePairComparator probComp =
                        tuplePairComparatorFactoryProbe2Build.createTuplePairComparator(ctx);
                final ITuplePairComparator buildComp =
                        tuplePairComparatorFactoryBuild2Probe.createTuplePairComparator(ctx);
                final IPredicateEvaluator predEvaluator =
                        predEvaluatorFactory == null ? null : predEvaluatorFactory.createPredicateEvaluator();

                final RecordDescriptor buildRd = recordDescProvider.getInputRecordDescriptor(buildAid, 0);
                final RecordDescriptor probeRd = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);

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
                            state.joiner.clearBuild();
                            state.joiner.clearProbe();
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
                            //if there are spilled buckets we need to do additional runs
                            //here we first need to create a heuristic class
                            //heuristic class should take the bucket table from the joiner and provide bucket iterators for build and probe
                            //buckets should give the
                            state.joiner.releaseResource();
                        }

                    } catch (Exception e) {
                        if (state.joiner != null) {
                            state.joiner.clearProbe();
                        }
                        // Since writer.nextFrame() is called in the above "try" body, we have to call writer.fail()
                        // to send the failure signal to the downstream, when there is a throwable thrown.
                        writer.fail();
                        // Clear temp files as this.fail() nor this.close() will no longer be called after close().
                        state.joiner.clearBuild();
                        state.joiner.clearProbe();
                        // Re-throw the whatever is caught.
                        throw e;
                    } finally {
                        try {
                            logProbeComplete();
                        } finally {

                        }
                    }

                    //If there is spilled records to disk
                    if (state.joiner.isSpilled()) {

                        LOGGER.info("Starting to process disk spilled buckets");

                        //state.joiner.getBucketTable().printInfo();
                        RunFileStream[] runFileStreams = new RunFileStream[2];
                        runFileStreams[0] = state.joiner.getRunFileStreamForBuild();
                        runFileStreams[1] = state.joiner.getRunFileStreamForProbe();

                        runFileStreams[0].startReadingRunFile();
                        runFileStreams[1].startReadingRunFile();

                        //Find the IORand and IOSeq here
                        double IORand = 275000;
                        double IOSeq = 60000;

                        /*int frameSizes = ctx.getInitialFrameSize();
                        IFrame framew = new VSizeFrame(ctx, frameSizes);
                        FrameTupleAccessor accessorBuild = new FrameTupleAccessor(buildRd);
                        int frameId = 0;
                        LinkedHashMap<Integer, Integer> bucketMap = new LinkedHashMap<>();
                        ArrayList<Integer> frameIds = new ArrayList<>();
                        for (int i = 0; i < runFileStreams[0].getWriteCount(); i++) {
                            frameIds.add(i);
                        }
                        long start = System.nanoTime();
                        while (frameIds.size() > 0) {
                            int currentFrame = frameIds.get((int) (Math.random() * frameIds.size()));
                            runFileStreams[0].seekToAPosition((long) currentFrame * frameSizes);
                            if (!runFileStreams[0].loadNextBuffer(framew))
                                break;
                            accessorBuild.reset(framew.getBuffer());
                            int tupleCount = accessorBuild.getTupleCount();
                            //System.out.println(frameId + ":" + tupleCount);
                            /*int bizzo = -1;
                            for (int i = 0; i < tupleCount; i++) {
                        
                                //                        if(accessorBuild.getTupleStartOffset(i) < startOffset) continue;
                                //                        if(endOffset != -1) {
                                //                            if(accessorBuild.getTupleStartOffset(i) >= endOffset) break;
                                //                        }
                                // b = FlexibleJoinsUtil.getBucketId(accessorBuild,i,1);
                                int bucketIdT = FlexibleJoinsUtil.getBucketId(accessorBuild, i, buildKeys[0]);
                                if(bizzo != -1 && bucketIdT != bizzo)
                                    System.out.println(bucketIdT);
                                if(bizzo == -1) bizzo = bucketIdT;
                        
                        
                                bucketMap.merge(bucketIdT, 1, Integer::sum);
                        
                            }
                            frameIds.remove(Integer.valueOf(currentFrame));
                        }
                        long end = System.nanoTime();
                        IORand = (double) (end - start) / runFileStreams[0].getWriteCount();
                        
                        /*StringBuilder a = new StringBuilder();
                        a.append("*****Bucket Counter From Build Side\n");
                        for(Integer bucketId: bucketMap.keySet()) {
                            a.append(bucketId).append("\t").append(bucketMap.get(bucketId)).append("\n");
                        }
                        System.out.println(a);
                        bucketMap.clear();
                        a = new StringBuilder();
                        FrameTupleAccessor accessorProbe = new FrameTupleAccessor(probeRd);
                        frameId = 0;
                        start = System.nanoTime();
                        while (runFileStreams[1].loadNextBuffer(framew)) {
                            accessorProbe.reset(framew.getBuffer());
                            int tupleCount = accessorProbe.getTupleCount();
                            //System.out.println(frameId + ":" + tupleCount);
                            /*for (int i = 0; i < tupleCount; i++) {
                        
                                //                        if(accessorBuild.getTupleStartOffset(i) < startOffset) continue;
                                //                        if(endOffset != -1) {
                                //                            if(accessorBuild.getTupleStartOffset(i) >= endOffset) break;
                                //                        }
                                // b = FlexibleJoinsUtil.getBucketId(accessorBuild,i,1);
                                int bucketIdT = FlexibleJoinsUtil.getBucketId(accessorProbe, i, probeKeys[0]);
                                //System.out.println(bucketIdT);
                                bucketMap.merge(bucketIdT, 1, Integer::sum);
                            }
                            frameId++;
                        }
                        end = System.nanoTime();
                        IOSeq = (double) (end - start) / runFileStreams[1].getWriteCount();
                        /*a.append("*****Bucket Counter From Probe Side\n");
                        for(Integer bucketId: bucketMap.keySet()) {
                            a.append(bucketId).append("\t").append(bucketMap.get(bucketId)).append("\n");
                        }
                        System.out.println(a);
                        LOGGER.info("Starting to process disk based buckets. \nBuild & Probe File Sizes: "
                                + runFileStreams[0].getRunFileReaderSize() / 1024 + "KB\t"
                                + runFileStreams[1].getRunFileReaderSize() / 1024 + "KB");
                        
                        //System.out.println("IOSeq:" + IOSeq);
                        //System.out.println("IORand:" + IORand);
                        //Create an instance of a heuristic with table and two file streams*/

                        //IHeuristicForThetaJoin heuristicForThetaJoin;

                        /*switch (heuristic) {
                            case "biggest-first":
                                heuristicForThetaJoin = new BigFirst(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, false, true);
                                break;
                            case "biggest-first-r":
                                heuristicForThetaJoin = new BigFirst(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, true, true);
                                break;
                            case "biggest-first-s":
                                heuristicForThetaJoin = new BigFirst(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, false, false);
                                break;
                            case "biggest-first-r-s":
                                heuristicForThetaJoin = new BigFirst(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, true, false);
                                break;
                            case "smallest-first":
                                heuristicForThetaJoin = new SmallFirst(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, false, true);
                                break;
                            case "smallest-first-r":
                                heuristicForThetaJoin = new SmallFirst(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, true, true);
                                break;
                            case "smallest-first-s":
                                heuristicForThetaJoin = new SmallFirst(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, false, false);
                                break;
                            case "smallest-first-r-s":
                                heuristicForThetaJoin = new SmallFirst(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, true, false);
                                break;
                            case "first-fit":
                                heuristicForThetaJoin = new FirstFit(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, false, true);
                                break;
                            case "first-fit-r":
                                heuristicForThetaJoin = new FirstFit(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, true, true);
                                break;
                            case "first-fit-s":
                                heuristicForThetaJoin = new FirstFit(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, false, false);
                                break;
                            case "first-fit-r-s":
                                heuristicForThetaJoin = new FirstFit(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, true, false);
                                break;
                            case "weighted":
                                heuristicForThetaJoin = new Weighted(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, false, true);
                                break;
                            case "weighted-r":
                                heuristicForThetaJoin = new Weighted(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, true, true);
                                break;
                            case "weighted-max":
                                heuristicForThetaJoin = new WeightedMax(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, false, true);
                                break;
                            case "weighted-max-r":
                                heuristicForThetaJoin = new WeightedMax(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, true, true);
                                break;
                            case "degree":
                                heuristicForThetaJoin = new Degree(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, false, false);
                                break;
                            case "degree-r":
                                heuristicForThetaJoin = new Degree(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, true, false);
                                break;
                            case "degree-s":
                                heuristicForThetaJoin = new Degree(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, false, true);
                                break;
                            case "degree-r-s":
                                heuristicForThetaJoin = new Degree(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, true, true);
                                break;
                            default:
                                heuristicForThetaJoin = new BigFirst(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                        runFileStreams[0].getRunFileReaderSize(),
                                        runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                        probeKeys, true, true);
                                break;
                        }*/

                        IHeuristicForThetaJoin[] heuristics = new IHeuristicForThetaJoin[6];

                        heuristics[5] = new BigFirst(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                runFileStreams[0].getRunFileReaderSize(), runFileStreams[1].getRunFileReaderSize(),
                                buildRd, probeRd, buildKeys, probeKeys, true, false);

                        heuristics[4] = new BigFirst(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                runFileStreams[0].getRunFileReaderSize(), runFileStreams[1].getRunFileReaderSize(),
                                buildRd, probeRd, buildKeys, probeKeys, true, true);

                        heuristics[3] = new SmallFirst(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                runFileStreams[0].getRunFileReaderSize(), runFileStreams[1].getRunFileReaderSize(),
                                buildRd, probeRd, buildKeys, probeKeys, true, false);

                        heuristics[2] = new SmallFirst(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                runFileStreams[0].getRunFileReaderSize(), runFileStreams[1].getRunFileReaderSize(),
                                buildRd, probeRd, buildKeys, probeKeys, true, true);

                        heuristics[1] = new FirstFit(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                runFileStreams[0].getRunFileReaderSize(), runFileStreams[1].getRunFileReaderSize(),
                                buildRd, probeRd, buildKeys, probeKeys, true, false);

                        heuristics[0] = new FirstFit(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                runFileStreams[0].getRunFileReaderSize(), runFileStreams[1].getRunFileReaderSize(),
                                buildRd, probeRd, buildKeys, probeKeys, true, true);

                        /*heuristics[3] = new Degree(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                runFileStreams[0].getRunFileReaderSize(),
                                runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                probeKeys, true, true);
                        
                        heuristics[4] = new WeightedMax(memoryForJoin - 1, ctx.getInitialFrameSize(),
                                runFileStreams[0].getRunFileReaderSize(),
                                runFileStreams[1].getRunFileReaderSize(), buildRd, probeRd, buildKeys,
                                probeKeys, true, true);*/

                        StringBuilder sb = new StringBuilder();

                        sb.append(
                                "\nHeuristic\tJoining Time(ms)\tIO Cost\tMatch Calls\tVerify Calls\tPartition\tH. Time");

                        for (IHeuristicForThetaJoin heuristicForThetaJoin : heuristics) {
                            StringBuilder fileContent = new StringBuilder();
                            StringBuilder fileContent1 = new StringBuilder();
                            StringBuilder fileContent2 = new StringBuilder();
                            StringBuilder fileContent3 = new StringBuilder();
                            StringBuilder fileContent4 = new StringBuilder();
                            StringBuilder fileContent5 = new StringBuilder();
                            //StringBuilder fileContent6 = new StringBuilder();
                            //StringBuilder fileContent7 = new StringBuilder();
                            //StringBuilder fileContent8 = new StringBuilder();
                            try {
                                File myObj = new File("/Volumes/Samsung_T5/user_dedup_1M.json");
                                Scanner myReader = new Scanner(myObj);
                                while (myReader.hasNextLine()) {
                                    String line = myReader.nextLine();
                                    fileContent.append(line);
                                    fileContent1.append(line);
                                    fileContent2.append(line);
                                    fileContent3.append(line);
                                    fileContent4.append(line);
                                    fileContent5.append(line);
                                    //fileContent6.append(line);
                                    //fileContent7.append(line);
                                    //fileContent8.append(line);
                                }
                                //                                File myObj2 = new File("tfj-build.waf");
                                //                                Scanner myReader2 = new Scanner(myObj2);
                                //                                while (myReader2.hasNextLine()) {
                                //                                    fileContent.append(myReader2.nextLine());
                                //                                }
                                myReader.close();
                                LOGGER.info("Loaded File Size: " + (long)(fileContent.length()+
                                        fileContent1.length()+
                                        fileContent2.length()+
                                        fileContent3.length()+
                                        fileContent4.length()+
                                        fileContent5.length()));
                                        //fileContent6.length()+
                                        //fileContent7.length()+
                                        //fileContent8.length()));
                            } catch (FileNotFoundException e) {
                                System.out.println("An error occurred.");
                                e.printStackTrace();
                            }

                            long heuristicStarts = System.nanoTime();
                            heuristicForThetaJoin.setComparator(probComp);
                            heuristicForThetaJoin.setBucketTable(state.joiner.getBucketTable());

                            heuristicForThetaJoin.setIOSeq(IOSeq / 1000000);
                            heuristicForThetaJoin.setIORnd(IORand / 1000000);
                            heuristicForThetaJoin.setIOSeek((IORand - IOSeq) / 1000000);
                            //((Weighted) heuristicForThetaJoin).knapsack();
                            heuristicForThetaJoin.simulate(false);
                            long hFinished = (System.nanoTime() - heuristicStarts) / 1000000;
                            //LOGGER.info("Heuristic Sim Finished In:" + (System.nanoTime() - heuristicStarts)/1000000);
                            long startJoining = System.nanoTime();
                            int iteration = 1;
                            int numberOfMatchCalls = 0;
                            int numberOfVerifyCalls = 0;
                            while (heuristicForThetaJoin.hasNextBuildingBucketSequence()) {
                                //state.joiner.getBucketTable().printInfo();
                                //get the building sequence buildSeq
                                ArrayList<Bucket> buildingBuckets = heuristicForThetaJoin.nextBuildingBucketSequence();
                                //LOGGER.info("Iteration " + iteration + ": Number of buckets " + buildingBuckets.size()
                                //+ " from side " + buildingBuckets.get(0).getSide());

                                //for each bucket from this sequence
                                InMemoryThetaFlexibleJoiner inMemoryThetaFlexibleJoiner = null;
                                for (int i = 0, buildingBucketsSize =
                                        buildingBuckets.size(); i < buildingBucketsSize; i++) {
                                    Bucket buildingBucket = buildingBuckets.get(i);
                                    int frameSize = ctx.getInitialFrameSize();
                                    IFrame frame = new VSizeFrame(ctx, frameSize);
                                    int startFrame = buildingBucket.getStartFrame();
                                    long endFrame = buildingBucket.getEndFrame();

                                    runFileStreams[buildingBucket.getSide()]
                                            .seekToAPosition((long) startFrame * frameSize);
                                    int currentFrame = startFrame;
                                    int startOffset;
                                    int endOffset;
                                    while (currentFrame < endFrame) {
                                        if (!runFileStreams[buildingBucket.getSide()].loadNextBuffer(frame))
                                            break;

                                        if (currentFrame == startFrame)
                                            startOffset = buildingBucket.getStartOffset();
                                        else
                                            startOffset = 5;
                                        if (currentFrame == endFrame)
                                            endOffset = buildingBucket.getEndOffset();
                                        else
                                            endOffset = -1;
                                        //TODO: build function should also get the record descriptor since it will not be always R
                                        if (inMemoryThetaFlexibleJoiner == null) {
                                            if (buildingBucket.getSide() == 0) {
                                                inMemoryThetaFlexibleJoiner = new InMemoryThetaFlexibleJoiner(ctx,
                                                        memoryForJoin, buildRd, probeRd, buildKeys, probeKeys,
                                                        buildingBuckets.size(), false);
                                            } else {
                                                inMemoryThetaFlexibleJoiner = new InMemoryThetaFlexibleJoiner(ctx,
                                                        memoryForJoin, probeRd, buildRd, probeKeys, buildKeys,
                                                        buildingBuckets.size(), true);
                                            }
                                        }

                                        inMemoryThetaFlexibleJoiner.buildOneBucket(frame.getBuffer(),
                                                buildingBucket.getBucketId(), startOffset, endOffset);
                                        currentFrame++;
                                    }

                                    //System.out.println("Reading a frame between " + startFrame + " - " + endFrame + " took average " + avgFrameReadForBucket);
                                }

                                //int probingSide = 1 - buildingBuckets.get(0).getSide();

                                //                            inMemoryThetaFlexibleJoiner.initProbe(buildComp);
                                //                            //TODO: we need to read only the matching buckets from probe side
                                //                            FrameTupleCursor frameTupleCursor = new FrameTupleCursor(probeRd);
                                //                            int probingSide = 1 - buildingBuckets.get(0).getSide();
                                //                            if (probingSide == 1)
                                //                                inMemoryThetaFlexibleJoiner.initProbe(probComp);
                                //                            else
                                //                                inMemoryThetaFlexibleJoiner.initProbe(buildComp);
                                //                            runFileStreams[probingSide].startReadingRunFile(frameTupleCursor);
                                //                            inMemoryThetaFlexibleJoiner.probeOneBucket(frameTupleCursor.getAccessor().getBuffer(),
                                //                                    writer, 5, -1);
                                //                            while (runFileStreams[probingSide].loadNextBuffer(frameTupleCursor)) {
                                //                                inMemoryThetaFlexibleJoiner.probeOneBucket(frameTupleCursor.getAccessor().getBuffer(),
                                //                                        writer, 5, -1);
                                //                            }

                                //get the probing sequence
                                ArrayList<Bucket> probingBuckets = heuristicForThetaJoin.nextProbingBucketSequence();

                                int probingSide = 1 - buildingBuckets.get(0).getSide();
                                if (probingSide == 1)
                                    inMemoryThetaFlexibleJoiner.initProbe(probComp);
                                else
                                    inMemoryThetaFlexibleJoiner.initProbe(buildComp);

                                //for each bucket p in probing sequence
                                for (Bucket probingBucket : probingBuckets) {
                                    int frameSize = ctx.getInitialFrameSize();
                                    IFrame frame = new VSizeFrame(ctx, frameSize);
                                    int startFrame = probingBucket.getStartFrame();
                                    long endFrame = probingBucket.getEndFrame();

                                    runFileStreams[probingBucket.getSide()]
                                            .seekToAPosition((long) startFrame * frameSize);
                                    int currentFrame = startFrame;
                                    int startOffset;
                                    int endOffset;
                                    while (currentFrame < endFrame) {
                                        if (!runFileStreams[probingBucket.getSide()].loadNextBuffer(frame))
                                            break;
                                        if (currentFrame == startFrame)
                                            startOffset = probingBucket.getStartOffset();
                                        else
                                            startOffset = 5;
                                        if (currentFrame == endFrame)
                                            endOffset = probingBucket.getEndOffset();
                                        else
                                            endOffset = -1;

                                        inMemoryThetaFlexibleJoiner.probeOneBucket(frame.getBuffer(), writer,
                                                startOffset, endOffset, false);

                                        currentFrame++;
                                    }
                                }

                                //inMemoryThetaFlexibleJoiner.getBucketTable().printInfo();

                                numberOfMatchCalls += inMemoryThetaFlexibleJoiner.getNumberOfMatchCalls();
                                numberOfVerifyCalls += inMemoryThetaFlexibleJoiner.getNumberOfVerifyCalls();

                                //inMemoryThetaFlexibleJoiner.completeProbe(writer);

                                inMemoryThetaFlexibleJoiner.releaseResource();

                                //state.joiner.getBucketTable().printInfo();
                                iteration++;
                            }

                            sb.append("\n" + heuristicForThetaJoin.getHeuristicName() + "\t"
                                    + (System.nanoTime() - startJoining) / 1000000 + "\t"
                                    + heuristicForThetaJoin.getTotalCost() + "\t" + numberOfMatchCalls + "\t"
                                    + numberOfVerifyCalls + "\t" + partition + "\t" + hFinished);
                            //System.out.println("Total calculated cost:" + ((Weighted) heuristicForThetaJoin).getTotalCost());
                            //}
                            /*while (heuristicForThetaJoin.hasNextBuildingBucketSequence()) {
                            //state.joiner.getBucketTable().printInfo();
                            //get the building sequence buildSeq
                            ArrayList<IBucket> buildingBuckets = heuristicForThetaJoin.nextBuildingBucketSequence();
                            LOGGER.info("Iteration " + iteration + ": Number of buckets " + buildingBuckets.size()
                                    + " from side " + buildingBuckets.get(0).getSide());
                            
                            //for each bucket from this sequence
                            InMemoryThetaFlexibleJoiner inMemoryThetaFlexibleJoiner = null;
                            for (int i = 0, buildingBucketsSize = buildingBuckets.size(); i < buildingBucketsSize; i++) {
                                IBucket buildingBucket = buildingBuckets.get(i);
                                int frameSize = ctx.getInitialFrameSize();
                                IFrame frame = new VSizeFrame(ctx, frameSize);
                                int startFrame = buildingBucket.getStartFrame();
                                long endFrame = buildingBucket.getEndFrame() == -1
                                        ? runFileStreams[buildingBucket.getSide()].getWriteCount()
                                        : buildingBucket.getEndFrame();
                            
                                runFileStreams[buildingBucket.getSide()].seekToAPosition((long) startFrame * frameSize);
                                int currentFrame = startFrame;
                                int startOffset;
                                int endOffset;
                                long start = System.nanoTime();
                                double avgFrameReadForBucket = 0;
                                while (currentFrame < endFrame) {
                                    if (!runFileStreams[buildingBucket.getSide()].loadNextBuffer(frame))
                                        break;
                                    long finish = System.nanoTime();
                                    avgFrameReadForBucket += finish - start;
                                    start = finish;
                            
                                    if (currentFrame == startFrame)
                                        startOffset = buildingBucket.getStartOffset();
                                    else
                                        startOffset = 5;
                                    if (currentFrame == endFrame)
                                        endOffset = buildingBucket.getEndOffset();
                                    else
                                        endOffset = -1;
                                    //TODO: build function should also get the record descriptor since it will not be always R
                                    if (inMemoryThetaFlexibleJoiner == null) {
                                        if (buildingBucket.getSide() == 0) {
                                            inMemoryThetaFlexibleJoiner = new InMemoryThetaFlexibleJoiner(ctx,
                                                    memoryForJoin, buildRd, probeRd, buildKeys, probeKeys,
                                                    buildingBuckets.size(), false);
                                        } else {
                                            inMemoryThetaFlexibleJoiner = new InMemoryThetaFlexibleJoiner(ctx,
                                                    memoryForJoin, probeRd, buildRd, probeKeys, buildKeys,
                                                    buildingBuckets.size(), true);
                                        }
                                    }
                            
                                    inMemoryThetaFlexibleJoiner.buildOneBucket(frame.getBuffer(),
                                            buildingBucket.getBucketId(), startOffset, endOffset);
                                    currentFrame++;
                                }
                                avgFrameReadForBucket /= (endFrame - startFrame);
                                if(i > 0 && buildingBuckets.get(i-1).getEndFrame() == buildingBucket.getStartFrame()) {
                                    avgSeq += avgFrameReadForBucket;
                                    countSeq++;
                                } else {
                                    avgRndm += avgFrameReadForBucket;
                                    countRnd++;
                                }
                                //System.out.println("Reading a frame between " + startFrame + " - " + endFrame + " took average " + avgFrameReadForBucket);
                            }
                            if(countRnd > 0 && countSeq > 0) {
                                heuristicForThetaJoin.setIORnd(avgRndm / (countRnd));
                                heuristicForThetaJoin.setIOSeq(avgSeq / (countSeq));
                                //System.out.println("IOSeq:" + avgRndm / (countRnd * 1000000));
                                //System.out.println("IORnd:" + avgSeq / (countSeq * 1000000));
                            }
                            
                            //int probingSide = 1 - buildingBuckets.get(0).getSide();
                            
                            //                            inMemoryThetaFlexibleJoiner.initProbe(buildComp);
                            //                            //TODO: we need to read only the matching buckets from probe side
                            //                            FrameTupleCursor frameTupleCursor = new FrameTupleCursor(probeRd);
                            //                            int probingSide = 1 - buildingBuckets.get(0).getSide();
                            //                            if (probingSide == 1)
                            //                                inMemoryThetaFlexibleJoiner.initProbe(probComp);
                            //                            else
                            //                                inMemoryThetaFlexibleJoiner.initProbe(buildComp);
                            //                            runFileStreams[probingSide].startReadingRunFile(frameTupleCursor);
                            //                            inMemoryThetaFlexibleJoiner.probeOneBucket(frameTupleCursor.getAccessor().getBuffer(),
                            //                                    writer, 5, -1);
                            //                            while (runFileStreams[probingSide].loadNextBuffer(frameTupleCursor)) {
                            //                                inMemoryThetaFlexibleJoiner.probeOneBucket(frameTupleCursor.getAccessor().getBuffer(),
                            //                                        writer, 5, -1);
                            //                            }
                            
                            //get the probing sequence
                            ArrayList<IBucket> probingBuckets = heuristicForThetaJoin.nextProbingBucketSequence();
                            
                            int probingSide = 1 - buildingBuckets.get(0).getSide();
                            if (probingSide == 1)
                                inMemoryThetaFlexibleJoiner.initProbe(probComp);
                            else
                                inMemoryThetaFlexibleJoiner.initProbe(buildComp);
                            
                            //for each bucket p in probing sequence
                            for (IBucket probingBucket : probingBuckets) {
                                int frameSize = ctx.getInitialFrameSize();
                                IFrame frame = new VSizeFrame(ctx, frameSize);
                                int startFrame = probingBucket.getStartFrame();
                                long endFrame = probingBucket.getEndFrame() == -1
                                        ? runFileStreams[probingBucket.getSide()].getWriteCount()
                                        : probingBucket.getEndFrame();
                            
                                runFileStreams[probingBucket.getSide()].seekToAPosition((long) startFrame * frameSize);
                                int currentFrame = startFrame;
                                int startOffset;
                                int endOffset;
                                while (currentFrame < endFrame) {
                                    if (!runFileStreams[probingBucket.getSide()].loadNextBuffer(frame))
                                        break;
                                    if (currentFrame == startFrame)
                                        startOffset = probingBucket.getStartOffset();
                                    else
                                        startOffset = 5;
                                    if (currentFrame == endFrame)
                                        endOffset = probingBucket.getEndOffset();
                                    else
                                        endOffset = -1;
                            
                                    inMemoryThetaFlexibleJoiner.probeOneBucket(frame.getBuffer(), writer, startOffset,
                                            endOffset);
                                    currentFrame++;
                                }
                            }
                            
                            //inMemoryThetaFlexibleJoiner.getBucketTable().printInfo();
                            
                            inMemoryThetaFlexibleJoiner.completeProbe(writer);
                            inMemoryThetaFlexibleJoiner.releaseResource();
                            
                            //state.joiner.getBucketTable().printInfo();
                            iteration++;
                            }
                            
                            //System.out.println("Total calculated cost:" + ((Weighted) heuristicForThetaJoin).getTotalCost());*/
                        }

                        LOGGER.info(sb);
                    }

                    writer.close();

                    /*LOGGER.info(partition + "\t" + state.joiner.getBucketTable().getNumEntries() + "\t"
                            + state.joiner.getBucketTable().getNumberOfBuildBuckets() + "\t"
                            + state.joiner.getBucketTable().getNumberOfProbeBuckets() + "\t"
                            + state.joiner.getNumberOfMatchCalls());*/

                }

                private void logProbeComplete() {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Flexible Join closed its probe phase");
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    failed = true;
                    if (state.joiner != null) {
                        state.joiner.clearBuild();
                        state.joiner.clearProbe();
                    }
                    writer.fail();
                }
            };
        }
    }
}
