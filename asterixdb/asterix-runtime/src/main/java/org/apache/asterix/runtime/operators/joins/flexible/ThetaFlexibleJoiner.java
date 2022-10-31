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
import java.util.Arrays;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.operators.joins.flexible.utils.memory.FlexibleJoinsSideTuple;
import org.apache.asterix.runtime.operators.joins.flexible.utils.memory.FlexibleJoinsUtil;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFilePointer;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFileStream;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.BucketBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.BufferInfo;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
import org.apache.hyracks.dataflow.std.structures.SerializableBucketIdList;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class ThetaFlexibleJoiner {

    private final RunFileStream runFileStreamForBuild;
    private final RunFilePointer runFilePointerForBuild;

    private final RunFileStream runFileStreamForProbe;
    private final RunFilePointer runFilePointerForProbe;

    private FlexibleJoinsSideTuple memoryTuple;
    private FlexibleJoinsSideTuple[] inputTuple;
    private TuplePointer tp;

    protected static final int JOIN_PARTITIONS = 2;
    protected static final int BUILD_PARTITION = 0;
    protected static final int PROBE_PARTITION = 1;

    protected final FrameTupleAppender resultAppender;

    private final IHyracksTaskContext ctx;

    private final BufferInfo tempInfo = new BufferInfo(null, -1, -1);

    private int memSizeInFrames;

    private ISimpleFrameBufferManager bufferManagerForHashTable;
    private BucketBufferManager bufferManager;
    private ITuplePointerAccessor memoryAccessor;

    private final FrameTupleAccessor accessorBuild;
    private final FrameTupleAccessor accessorProbe;
    private final TuplePointer tempPtr = new TuplePointer();

    private ITuplePairComparator tpComparator;

    private final String buildRelName;
    private final String probeRelName;

    private final RecordDescriptor buildRd;
    private final RecordDescriptor probeRd;

    private final IPredicateEvaluator predEvaluator;
    private final int nBuckets;
    private boolean isCurrentBucketSpilled;

    private SerializableBucketIdList table;

    private Integer currentBucketId;
    private boolean newBucket;

    private boolean memoryOpen;
    private int latestBucketInMemory;
    private Integer previousLatestBucketInMemory;

    //        private LinkedHashMap<Integer, Integer> bucketMap = new LinkedHashMap<>();
    //        private LinkedHashMap<Integer, Integer> bucketMatchCount = new LinkedHashMap<>();
    //    //    private LinkedHashMap<Integer, Long> spilledBucketMap = new LinkedHashMap<>();

    protected int numberOfBuckets = 0;

    protected boolean spilled;

    protected long numRecordsFromBuild;

    public ThetaFlexibleJoiner(IHyracksTaskContext ctx, int memorySize, RecordDescriptor buildRd,
            RecordDescriptor probeRd, String probeRelName, String buildRelName, IPredicateEvaluator predEval,
            int nBuckets) throws HyracksDataException {

        // Memory (probe buffer)
        if (memorySize < 5) {
            throw new RuntimeException(
                    "FlexibleJoiner does not have enough memory (needs > 4, got " + memorySize + ").");
        }
        this.ctx = ctx;
        this.nBuckets = nBuckets;

        this.buildRd = buildRd;
        this.probeRd = probeRd;

        // Run File and frame cache (build buffer)
        runFileStreamForBuild = new RunFileStream(ctx, "tfj-build");
        runFilePointerForBuild = new RunFilePointer();
        runFileStreamForBuild.createRunFileWriting();
        runFileStreamForBuild.startRunFileWriting();

        // Run File and frame cache (probe buffer)
        runFileStreamForProbe = new RunFileStream(ctx, "tfj-probe");
        runFilePointerForProbe = new RunFilePointer();
        runFileStreamForProbe.createRunFileWriting();
        runFileStreamForProbe.startRunFileWriting();

        // Result
        this.resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));

        this.memSizeInFrames = memorySize;
        this.accessorBuild = new FrameTupleAccessor(buildRd);
        this.accessorProbe = new FrameTupleAccessor(probeRd);

        this.buildRelName = buildRelName;
        this.probeRelName = probeRelName;

        this.predEvaluator = predEval;

        IDeallocatableFramePool framePool =
                new DeallocatableFramePool(ctx, memSizeInFrames * ctx.getInitialFrameSize());
        bufferManagerForHashTable = new FramePoolBackedFrameBufferManager(framePool);

        table = new SerializableBucketIdList(nBuckets, ctx, bufferManagerForHashTable);

        this.bufferManager = new BucketBufferManager(framePool, buildRd);
        this.memoryAccessor = bufferManager.createTuplePointerAccessor();

        this.memoryOpen = true;
        this.spilled = false;
        this.numRecordsFromBuild = 0;

    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        accessorBuild.reset(buffer);
        int tupleCount = accessorBuild.getTupleCount();
        numRecordsFromBuild += tupleCount;
        //currentBucketId = -1;
        for (int i = 0; i < tupleCount; i++) {
            boolean inMemory = false;
            boolean writeToDisk = false;
            newBucket = false;
            int bucketId = FlexibleJoinsUtil.getBucketId(accessorBuild, i, 1);
            TuplePointer tuplePointer = new TuplePointer();
            //if(bucketId != currentBucketId) {
            //currentBucketId = bucketId;
            tuplePointer = table.getBuildTuplePointer(bucketId);
            if (tuplePointer == null) {
                newBucket = true;
                tuplePointer = new TuplePointer();
                writeToDisk = false;
            } else if (tuplePointer.getFrameIndex() < 0) {
                newBucket = false;
                writeToDisk = true;
            } else {
                newBucket = false;
                writeToDisk = false;
            }

            //}

            if (writeToDisk) {
                runFileStreamForBuild.addToRunFile(accessorBuild, i, tuplePointer, false);
            } else {
                while (!bufferManager.insertTuple(accessorBuild, i, tuplePointer)) {
                    int lastBucket = table.lastBucket();
                    TuplePointer tuplePointerToSpill = table.getBuildTuplePointer(lastBucket);
                    TuplePointer newTuplePointerForSpilled = spillStartingFrom(tuplePointerToSpill);

                    //runFileStreamForBuild.addToRunFile(accessorBuild, i, tuplePointer);
                    table.updateBuildBucket(lastBucket, newTuplePointerForSpilled);
                    //spilledBucketMap.put(lastBucket, runFileStreamForBuild.getTupleCount());
                    if (!newBucket) {
                        runFileStreamForBuild.addToRunFile(accessorBuild, i, tuplePointer, false);
                        break;
                    }
                }
            }

            if (newBucket) {
                if (!table.insert(bucketId, tuplePointer, new TuplePointer())) {

                    bufferManager.cancelLastInsert();

                    int lastBucket = table.lastBucket();
                    TuplePointer tuplePointerToSpill = table.getBuildTuplePointer(lastBucket);
                    TuplePointer newTuplePointerForSpilled = spillStartingFrom(tuplePointerToSpill);
                    table.updateBuildBucket(lastBucket, newTuplePointerForSpilled);
                    bufferManager.insertTuple(accessorBuild, i, tuplePointer);
                    table.insert(bucketId, tuplePointer, new TuplePointer());

                }
                //bucketMap.put(bucketId, 0);
            }
            //bucketMap.merge(bucketId, 1, Integer::sum);
        }
    }

    public void clearBuild() throws HyracksDataException {
        runFileStreamForBuild.removeRunFile();
    }

    public void clearProbe() throws HyracksDataException {
        runFileStreamForProbe.removeRunFile();
    }

    public void closeBuild() throws HyracksDataException {
        //                System.out.println("Number of records from build side: " + numRecordsFromBuild);
        //
        //                StringBuilder a = new StringBuilder();
        //                a.append("Bucket Counter From Build Side\n");
        //                for(Integer bucketId: bucketMap.keySet()) {
        //                    a.append(bucketId).append("\t").append(bucketMap.get(bucketId)).append("\n");
        //                }
        //                System.out.println(a);
        /*System.out.println("\nSpilled Map");
        StringBuilder b = new StringBuilder();
        for(Integer bucketId: spilledBucketMap.keySet()) {
            b.append(bucketId).append("\t").append(spilledBucketMap.get(bucketId)).append("\n");
        }
        System.out.println(b);*/
        table.printInfo();
        runFileStreamForBuild.flushRunFile();
    }

    private TuplePointer spillStartingFrom(TuplePointer tuplePointer) throws HyracksDataException {
        //spill the records from the memory starting from the given tuple pointer
        //return a tuple pointer that shows the starting location on disk
        //Starting from the tuple pointer, read records from memory
        TuplePointer returnTuplePointer = new TuplePointer();
        int fIndex = tuplePointer.getFrameIndex();
        int tIndex = tuplePointer.getTupleIndex();
        boolean firstFrame = true;
        //IAppendDeletableFrameTupleAccessor frameTupleAccessor = new DeletableFrameTupleAppender(buildRd);
        //ITuplePointerAccessor tupleAccessor = bufferManager.createTuplePointerAccessor();
        /*memoryAccessor.reset(tuplePointer);
        for(int i = 0; i < memoryAccessor.getTupleCount(); i++) {
            if(i == 0) {
                runFileStreamForBuild.addToRunFile(memoryAccessor, i, returnTuplePointer);
            }
            else {
                runFileStreamForBuild.addToRunFile(memoryAccessor, i);
            }
        
        }*/
        int writeCounter = 0;
        FrameTupleAccessor frameTupleAccessor = new FrameTupleAccessor(buildRd);
        while (fIndex < bufferManager.getNumberOfFrames()) {
            bufferManager.getFrame(fIndex, tempInfo);
            //tempInfo.getBuffer().position(tempInfo.getStartOffset());
            //tempInfo.getBuffer().limit(tempInfo.getStartOffset() + tempInfo.getLength());
            frameTupleAccessor.reset(tempInfo.getBuffer());
            int i = firstFrame ? tIndex : 0;
            while (i < frameTupleAccessor.getTupleCount()) {
                if (firstFrame) {
                    runFileStreamForBuild.addToRunFile(frameTupleAccessor, i, returnTuplePointer, true);
                    firstFrame = false;
                } else {
                    runFileStreamForBuild.addToRunFile(frameTupleAccessor, i);
                }
                writeCounter++;
                i++;
            }
            fIndex++;
        }
        bufferManager.removeBucket(tuplePointer);
        /*tupleAccessor.reset(tuplePointer);
        for(int i = 0; i < tupleAccessor.getTupleCount(); i++) {
            bufferManager.deleteTuple();
        
        
        }*/
        //bufferManager.reOrganizeFrames();
        //System.out.println(writeCounter);
        return returnTuplePointer;
    }

    public void initProbe(ITuplePairComparator comparator) {
        this.tpComparator = comparator;
    }

    private byte[] intToByteArray(int value) {
        return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
    }

    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount = accessorProbe.getTupleCount();
        int numberOfBuckets = table.getNumEntries();

        IFrameTupleAccessor iFrameTupleAccessor = null;
        IFrameTupleAccessor dumpTupleAccessorForBucket1 =
                new FrameTupleAccessor(buildRd);
        int accessorIndex = 0;
        int bucketCheck = 0;
        currentBucketId = -1;
        //For each s from S
        for (int i = 0; i < tupleCount; ++i) {

            int probeBucketId = FlexibleJoinsUtil.getBucketId(accessorProbe, i, 1);
            if (probeBucketId != currentBucketId) {
                bucketCheck = 0;
                currentBucketId = probeBucketId;
            }
            boolean writtenToDisk = false;
            //For each bucket from bucket table
            for (int bucketIndex = 0; bucketIndex < numberOfBuckets; bucketIndex++) {
                int matched = ((bucketCheck >> bucketIndex) & 1);

                int[] bucketInfo = table.getEntry(bucketIndex);
                //if the building tuple pointer has a negative tuple index that means we added this bucket only from S side
                if (bucketInfo[2] == -1)
                    continue;

                if (matched == 0) {

                    //Below we need to create appropriate accessor to use it in comparator
                    byte[] dumpArray = new byte[buildRd.getFieldCount() * 4 + 5 + 5];
                    ByteBuffer buff = ByteBuffer.wrap(dumpArray);
                    buff.position(dumpArray.length - 5);
                    buff.put(ATypeTag.INTEGER.serialize());
                    buff.putInt(bucketInfo[0]);
                    //buff.position(0);
                    dumpTupleAccessorForBucket1.reset(buff);
                    iFrameTupleAccessor = dumpTupleAccessorForBucket1;
                }
                //int bucki = FlexibleJoinsUtil.getBucketId(iFrameTupleAccessor, 0, 1);
                //If buckets are matching
                if (matched == 1 || this.tpComparator.compare(iFrameTupleAccessor, 0, accessorProbe, i) < 1) {

                    bucketCheck = bucketCheck | (1 << bucketIndex);
                    //If the building bucket is in memory we join the records
                    if (bucketInfo[1] > -1) {
                        int tupleCounter = bucketInfo[2];
                        int frameCounter = bucketInfo[1];
                        memoryAccessor.reset(new TuplePointer(frameCounter, tupleCounter));
                        boolean finished = false;
                        boolean first = true;
                        while (frameCounter < bufferManager.getNumberOfFrames()) {
                            if (!first) {
                                tupleCounter = 0;
                            }
                            while (tupleCounter < memoryAccessor.getTupleCount()) {
                                first = false;
                                memoryAccessor.reset(new TuplePointer(frameCounter, tupleCounter));
                                int bucketReadFromMem = FlexibleJoinsUtil.getBucketId(memoryAccessor, tupleCounter, 1);
                                if (bucketReadFromMem != bucketInfo[0]) {
                                    finished = true;
                                    break;
                                }
                                addToResult(memoryAccessor, tupleCounter, accessorProbe, i, writer);
                                tupleCounter++;

                            }
                            if (finished)
                                break;
                            frameCounter++;
                        }
                    } else if (!writtenToDisk) {

                        TuplePointer tuplePointerTester = table.getProbeTuplePointer(probeBucketId);
                        boolean isBucketNew = (tuplePointerTester == null) || (tuplePointerTester.getFrameIndex() == -1
                                && tuplePointerTester.getTupleIndex() == -1);
                        //If the building bucket is on disk, we need to write s to disk but only once
                        TuplePointer tuplePointerForProbeDiskFile = new TuplePointer();
                        runFileStreamForProbe.addToRunFile(accessorProbe, i, tuplePointerForProbeDiskFile, isBucketNew);

                        if (tuplePointerTester == null) {
                            // set the tuple pointer for probe side as it locates it on disk
                            table.insert(probeBucketId, new TuplePointer(), tuplePointerForProbeDiskFile);
                        } else if (tuplePointerTester.getFrameIndex() == -1
                                && tuplePointerTester.getTupleIndex() == -1) {
                            table.updateProbeBucket(probeBucketId, tuplePointerForProbeDiskFile);
                        }
                        writtenToDisk = true;
                        this.spilled = true;
                    }
                }

            }
            //bucketMatchCount.merge(probeBucketId, 1, Integer::sum);
        }

    }

    public void completeProbe(IFrameWriter writer) throws HyracksDataException {
        //We do NOT join the spilled partitions here, that decision is made at the descriptor level
        //(which join technique to use)

        if (spilled) {
            //table.printInfo();
            runFileStreamForProbe.flushRunFile();
            runFileStreamForBuild.flushRunFile();
            //runFileStreamForProbe.startReadingRunFile(inputCursor[BUILD_PARTITION]);
        }

        //                table.printInfo();
        //                StringBuilder a = new StringBuilder();
        //                a.append("Bucket Counter From Probe Side\n");
        //                for(Integer bucketId: bucketMatchCount.keySet()) {
        //                    a.append(bucketId).append("\t").append(bucketMatchCount.get(bucketId)).append("\n");
        //                }
        //                System.out.println(a);
        resultAppender.write(writer, true);
    }

    public void releaseResource() throws HyracksDataException {

        bufferManager.close();
        bufferManager = null;
        //        bufferManagerForHashTable = null;
        //        table.reset();
    }

    private void addToResult(IFrameTupleAccessor buildAccessor, int buildTupleId, IFrameTupleAccessor probeAccessor,
            int probeTupleId, IFrameWriter writer) throws HyracksDataException {

        FrameUtils.appendConcatToWriter(writer, resultAppender, buildAccessor, buildTupleId, probeAccessor,
                probeTupleId);

    }

    public boolean isSpilled() {
        return spilled;
    }

    public RunFileStream getRunFileStreamForBuild() {
        return runFileStreamForBuild;
    }

    public RunFileStream getRunFileStreamForProbe() {
        return runFileStreamForProbe;
    }

    public void printTableInfo() {
        table.printInfo();
    }

    public SerializableBucketIdList getBucketTable() {
        return table;
    }

}
