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
import java.util.ArrayList;

import org.apache.asterix.runtime.operators.joins.flexible.utils.IBucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IHeuristicForThetaJoin;
import org.apache.asterix.runtime.operators.joins.flexible.utils.heuristics.BigFirst;
import org.apache.asterix.runtime.operators.joins.flexible.utils.heuristics.FirstFit;
import org.apache.asterix.runtime.operators.joins.flexible.utils.heuristics.SmallFirst;
import org.apache.asterix.runtime.operators.joins.flexible.utils.heuristics.Weighted;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.FrameTupleCursor;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFileStream;
import org.apache.http.cookie.SM;
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

public class ThetaFlexibleJoinOperatorDescriptor extends AbstractOperatorDescriptor {

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

    public ThetaFlexibleJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memoryForJoin, int[] buildKeys,
            int[] probeKeys, RecordDescriptor recordDescriptor, ITuplePairComparatorFactory tupPaircomparatorFactory01, ITuplePairComparatorFactory tupPaircomparatorFactory10,
            IPredicateEvaluatorFactory predEvaluatorFactory, double fudgeFactor) {
        super(spec, 2, 1);

        this.predEvaluatorFactory = predEvaluatorFactory;
        outRecDescs[0] = recordDescriptor;
        this.buildKeys = buildKeys;
        this.probeKeys = probeKeys;
        this.memoryForJoin = memoryForJoin;

        this.tuplePairComparatorFactoryProbe2Build = tupPaircomparatorFactory01;
        this.tuplePairComparatorFactoryBuild2Probe = tupPaircomparatorFactory10;

        this.fudgeFactor = fudgeFactor;
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
                            predEvaluator, 1000);

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
                final ITuplePairComparator buildComp = tuplePairComparatorFactoryBuild2Probe.createTuplePairComparator(ctx);
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
                        LOGGER.info("Starting to process disk based buckets");
                        //state.joiner.getBucketTable().printInfo();
                        RunFileStream[] runFileStreams = new RunFileStream[2];
                        runFileStreams[0] = state.joiner.getRunFileStreamForBuild();
                        runFileStreams[1] = state.joiner.getRunFileStreamForProbe();

                        runFileStreams[0].startReadingRunFile();
                        runFileStreams[1].startReadingRunFile();

                        //Create an instance of a heuristic with table and two file streams
                        Weighted heuristicForThetaJoin = new Weighted(memoryForJoin - 1,
                                ctx.getInitialFrameSize(), runFileStreams[0].getRunFileReaderSize(),
                                runFileStreams[1].getRunFileReaderSize(),
                                buildRd, probeRd);

                        heuristicForThetaJoin.setComparator(probComp);
                        heuristicForThetaJoin.setBucketTable(state.joiner.getBucketTable());
                        int iteration = 1;
                        while (heuristicForThetaJoin.hasNextBuildingBucketSequence()) {
                            //state.joiner.getBucketTable().printInfo();
                            //get the building sequence buildSeq
                            ArrayList<IBucket> buildingBuckets = heuristicForThetaJoin.nextBuildingBucketSequence();
                            LOGGER.info("Iteration " + iteration + ": Number of buckets "+buildingBuckets.size()+" from side " + buildingBuckets.get(0).getSide());


                            //for each bucket from this sequence
                            InMemoryThetaFlexibleJoiner inMemoryThetaFlexibleJoiner = null;
                            for (IBucket buildingBucket : buildingBuckets) {
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
                                  if(inMemoryThetaFlexibleJoiner == null) {
                                      if(buildingBucket.getSide() == 0) {
                                          inMemoryThetaFlexibleJoiner = new InMemoryThetaFlexibleJoiner(
                                                  ctx, memoryForJoin, buildRd, probeRd, buildingBuckets.size());
                                      } else {
                                          inMemoryThetaFlexibleJoiner = new InMemoryThetaFlexibleJoiner(
                                                  ctx, memoryForJoin, probeRd, buildRd, buildingBuckets.size());
                                      }
                                  }

                                  inMemoryThetaFlexibleJoiner.buildOneBucket(frame.getBuffer(),
                                          buildingBucket.getBucketId(), startOffset, endOffset);
                                  currentFrame++;
                              }
                          }
//                            //TODO: we need to read only the matching buckets from probe side
//                            FrameTupleCursor frameTupleCursor = new FrameTupleCursor(probeRd);
//                            int probingSide = 1 - buildingBuckets.get(0).getSide();
//                            if(probingSide == 1) inMemoryThetaFlexibleJoiner.initProbe(probComp);
//                            else inMemoryThetaFlexibleJoiner.initProbe(buildComp);
//                            runFileStreams[probingSide].startReadingRunFile(frameTupleCursor);
//                            inMemoryThetaFlexibleJoiner.probeOneBucket(frameTupleCursor.getAccessor().getBuffer(),
//                                    writer, 5, -1);
//                            while (runFileStreams[probingSide].loadNextBuffer(frameTupleCursor)) {
//                                inMemoryThetaFlexibleJoiner.probeOneBucket(frameTupleCursor.getAccessor().getBuffer(),
//                                        writer, 5, -1);
//                            }

                            // get the probing sequence
                            ArrayList<IBucket> probingBuckets = heuristicForThetaJoin.nextProbingBucketSequence();

                            int probingSide = 1 - buildingBuckets.get(0).getSide();
                            if(probingSide == 1) inMemoryThetaFlexibleJoiner.initProbe(probComp);
                            else inMemoryThetaFlexibleJoiner.initProbe(buildComp);

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

                                    inMemoryThetaFlexibleJoiner.probeOneBucket(frame.getBuffer(), writer, startOffset, endOffset);
                                    currentFrame++;
                                }
                            }

                            inMemoryThetaFlexibleJoiner.completeProbe(writer);
                            inMemoryThetaFlexibleJoiner.releaseResource();

                            //state.joiner.getBucketTable().printInfo();
                            iteration++;
                        }
                    }
                    writer.close();

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
