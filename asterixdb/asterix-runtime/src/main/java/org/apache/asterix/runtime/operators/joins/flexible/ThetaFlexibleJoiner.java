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

import it.unimi.dsi.fastutil.Hash;
import org.apache.asterix.runtime.operators.joins.flexible.utils.memory.FlexibleJoinsSideTuple;
import org.apache.asterix.runtime.operators.joins.flexible.utils.memory.FlexibleJoinsUtil;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.FrameTupleCursor;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFilePointer;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFileStream;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.TuplePointerCursor;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalGroupingProperty;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.BucketBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.BufferInfo;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
import org.apache.hyracks.dataflow.std.sort.util.DeletableFrameTupleAppender;
import org.apache.hyracks.dataflow.std.sort.util.IAppendDeletableFrameTupleAccessor;
import org.apache.hyracks.dataflow.std.structures.SerializableBucketIdList;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;

public class ThetaFlexibleJoiner {

    private final FrameTupleAccessor accessorOuter;

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
    protected final FrameTupleCursor[] inputCursor;

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

    protected int numberOfBuckets = 0;

    protected boolean spilled;

    protected long numRecordsFromBuild;


    public ThetaFlexibleJoiner(IHyracksTaskContext ctx,
                               int memorySize,
                               RecordDescriptor buildRd,
                               RecordDescriptor probeRd,
                               String probeRelName,
                               String buildRelName,
                               IPredicateEvaluator predEval,
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

        inputCursor = new FrameTupleCursor[JOIN_PARTITIONS];
        inputCursor[BUILD_PARTITION] = new FrameTupleCursor(buildRd);
        accessorOuter = new FrameTupleAccessor(probeRd);

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
        tempPtr.reset(-1, -1);
        for (int i = 0; i < tupleCount; ++i) {
            int bucketId = FlexibleJoinsUtil.getBucketId(accessorBuild, i, 1);
            //Check if we have a new bucket
            if (currentBucketId == null || !currentBucketId.equals(bucketId)) {
                if (table.getBuildTuplePointer(bucketId) == null) {
                    newBucket = true;
                    currentBucketId = bucketId;
                    isCurrentBucketSpilled = false;
                } else {
                    newBucket = false;
                }
            } else {
                newBucket = false;
            }
            // If the current bucket is spilled or memory is closed
            if (isCurrentBucketSpilled || !memoryOpen) {
                // Write the record to disk
                runFileStreamForBuild.addToRunFile(accessorBuild, i, tempPtr);
            } else {
                // If the memory does not accept the new record
                if (!bufferManager.insertTuple(accessorBuild, i, tempPtr)) {
                    // If this is the first record of the bucket
                    if (newBucket) {
                        //if we cannot insert a new bucket we close memory
                        memoryOpen = false;
                        //write record to disk directly and set the tuple pointer accordingly
                        runFileStreamForBuild.addToRunFile(accessorBuild, i, tempPtr);
                    } else {
                        // System.out.println("Bucket "+ bucketId+" is spilled!");
                        TuplePointer tuplePointerForspilledBucket = table.getBuildTuplePointer(bucketId);
                        //spill current bucket to disk
                        tempPtr.reset(spillStartingFrom(tuplePointerForspilledBucket));
                        //spill the rest of the records for this bucket
                        isCurrentBucketSpilled = true;
                        //change the bucket table to show the bucket is on disk
                        table.updateBuildBucket(bucketId, tempPtr);
                    }
                } else if (newBucket) {
                    previousLatestBucketInMemory = latestBucketInMemory;
                    latestBucketInMemory = bucketId;
                }

            }
            //insert new bucket to the bucket table
            if (newBucket) {
                numberOfBuckets++;
                if (!table.insert(bucketId, tempPtr, new TuplePointer())) {
                    //if there is not enough space for the new entry
                    if (!memoryOpen) {
                        //Memory is closed which means we already wrote the new bucket to disk
                        //We do not need to spill current bucket but only the latest bucket written to memory
                        latestBucketInMemory = table.lastBucket();
                        //System.out.println("Bucket "+ latestBucketInMemory+" is spilled!");
                        TuplePointer spilledBucketTuplePointer = new TuplePointer();
                        //table.printInfo();
                        spilledBucketTuplePointer.reset(spillStartingFrom(table.getBuildTuplePointer(latestBucketInMemory)));
                        //set the disk location for spilled bucket
                        table.updateBuildBucket(latestBucketInMemory, spilledBucketTuplePointer);
                        //re-open the memory since we might have enough space for future buckets
                        memoryOpen = true;

                    } else {
                        previousLatestBucketInMemory = table.lastBucket();
                        tempPtr.reset(spillStartingFrom(tempPtr));
                        System.out.println("new Bucket " + bucketId + " is spilled!");
                        //here we know we inserted only one record for this bucket to memory
                        // and the latest bucket is the current bucket
                        //1.we first need to spill the previous latest bucket to disk
                        TuplePointer spilledBucketTuplePointer = new TuplePointer();

                        //System.out.println("prev Bucket "+ previousLatestBucketInMemory+" is spilled!");
                        //table.printInfo();
                        spilledBucketTuplePointer.reset(spillStartingFrom(table.getBuildTuplePointer(previousLatestBucketInMemory)));
                        //2.then we need to change its tuple pointer
                        table.updateBuildBucket(previousLatestBucketInMemory, spilledBucketTuplePointer);
                        previousLatestBucketInMemory = null;
                        //3.next we spill current bucket to disk and change the tempPtr to show the location on disk
                        //tempPtr.reset(spillStartingFrom(tempPtr));
                    }
                    // re-try to insert new bucket to bucket table
                    while (!table.insert(bucketId, tempPtr, new TuplePointer())) {
                        latestBucketInMemory = table.lastBucket();
                        System.out.println("Bucket aaa " + latestBucketInMemory + " is spilled!");
                        TuplePointer spilledBucketTuplePointer = new TuplePointer();
                        //table.printInfo();
                        spilledBucketTuplePointer.reset(spillStartingFrom(table.getBuildTuplePointer(latestBucketInMemory)));
                        //set the disk location for spilled bucket
                        table.updateBuildBucket(latestBucketInMemory, spilledBucketTuplePointer);
                        //table.printInfo();
                        //re-open the memory since we might have enough space for future buckets
                        memoryOpen = true;
                    }
                }
            }
        }
    }

    public void clearBuild() throws HyracksDataException {
        runFileStreamForBuild.removeRunFile();
    }

    public void clearProbe() throws HyracksDataException {
        runFileStreamForProbe.removeRunFile();
    }

    public void closeBuild() throws HyracksDataException {
        //System.out.println("Number of records from build side: " + numRecordsFromBuild);
        //table.printInfo();
        runFileStreamForBuild.flushRunFile();
        runFileStreamForBuild.startReadingRunFile(inputCursor[BUILD_PARTITION]);
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
        FrameTupleAccessor frameTupleAccessor = new FrameTupleAccessor(buildRd);
        while (fIndex < bufferManager.getNumberOfFrames()) {
            bufferManager.getFrame(fIndex, tempInfo);
            tempInfo.getBuffer().position(tempInfo.getStartOffset());
            tempInfo.getBuffer().limit(tempInfo.getStartOffset() + tempInfo.getLength());
            frameTupleAccessor.reset(tempInfo.getBuffer());
            int i = firstFrame ? tIndex : 0;
            while (i < frameTupleAccessor.getTupleCount()) {
                if (firstFrame) {
                    runFileStreamForBuild.addToRunFile(frameTupleAccessor, i, returnTuplePointer);
                    firstFrame = false;
                } else {
                    runFileStreamForBuild.addToRunFile(frameTupleAccessor, i);
                }
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
        return returnTuplePointer;
    }

    public void initProbe(ITuplePairComparator comparator) {
        this.tpComparator = comparator;
    }

    private byte[] intToByteArray(int value) {
        return new byte[]{
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value};
    }

    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount = accessorProbe.getTupleCount();
        Integer currentBucket = null;
        boolean newBucket;
        IFrameTupleAccessor iFrameTupleAccessor;
        IFrameTupleAccessor dumpTupleAccessorForBucket1 = new FrameTupleAccessor(new RecordDescriptor(Arrays.copyOfRange(buildRd.getFields(), 0, 1), Arrays.copyOfRange(buildRd.getTypeTraits(), 0, 1)));
        //ITuplePointerAccessor memoryAccessor = bufferManager.createTuplePointerAccessor();
        BitSet bucketTest = new BitSet(numberOfBuckets);
        int accessorIndex = 0;
        // for each record from S
        for (int i = 0; i < tupleCount; ++i) {
            int probeBucketId = FlexibleJoinsUtil.getBucketId(accessorProbe, i, 1);

            if (currentBucket == null || !currentBucket.equals(probeBucketId)) {
                TuplePointer tuplePointerTester = table.getProbeTuplePointer(probeBucketId);
                if (tuplePointerTester == null || tuplePointerTester.getTupleIndex() == -1) {
                    currentBucket = probeBucketId;
                    newBucket = true;
                    bucketTest.clear();
                } else {
                    newBucket = false;
                }
            } else {
                newBucket = false;
            }
            boolean writtenToDisk = false;
            int numberOfBuckets = table.getNumEntries();
            // Iterate over the buckets from bucket table
            HashMap<Integer, Integer> bCounter = new HashMap<>();
            for (int bucketIndex = 0; bucketIndex < numberOfBuckets; bucketIndex++) {
                int[] bucketInfo = table.getEntry(bucketIndex);
                if (bucketInfo[2] == -1) continue;
                if (bucketInfo[1] > -1) {
                    memoryAccessor.reset(new TuplePointer(bucketInfo[1], bucketInfo[2]));
                    iFrameTupleAccessor = memoryAccessor;
                    accessorIndex = bucketInfo[2];
                } else {
                    dumpTupleAccessorForBucket1.reset(ByteBuffer.wrap(intToByteArray(bucketInfo[0])));
                    iFrameTupleAccessor = dumpTupleAccessorForBucket1;
                    if (writtenToDisk) continue;
                }
                // if buckets are matching
                //TODO Here we are not able to reach the data from memory if the bucket is already spilled
                if (bucketTest.get(bucketIndex) || this.tpComparator.compare(iFrameTupleAccessor, accessorIndex, accessorProbe, i) < 1) {
                    bucketTest.set(bucketIndex);
                    // check the tuple pointer of the build side
                    if (bucketInfo[1] > -1) {
                        // if the bucket is in memory join the records
                        int tupleCounter = bucketInfo[2];
                        int frameCounter = bucketInfo[1];
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
                                //if(bCounter.containsKey(bucketReadFromMem)) bCounter.put(bucketReadFromMem, bCounter.get(bucketReadFromMem) + 1);
                                //else bCounter.put(bucketReadFromMem, 1);
                                tupleCounter++;

                            }
                            if (finished) break;
                            frameCounter++;
                        }
                        //System.out.println("bCounter\n");
                        //bCounter.forEach((key, value) -> System.out.println(key + " " + value));


                    } else if (!writtenToDisk) {
                        //Set spilled to true to complete join using the records from disk
                        spilled = true;

                        writtenToDisk = true;
                        // if the bucket is on disk, write the bucket from probe side to disk
                        TuplePointer tuplePointerForProbeDiskFile = new TuplePointer();
                        runFileStreamForProbe.addToRunFile(accessorProbe, i, tuplePointerForProbeDiskFile);
                        if (newBucket) {
                            // set the tuple pointer for probe side as it locates it on disk
                            if (!table.updateProbeBucket(probeBucketId, tuplePointerForProbeDiskFile)) {
                                table.insert(probeBucketId, new TuplePointer(), tuplePointerForProbeDiskFile);
                            }
                        }
                    }
                }

            }

        }

    }

    public void completeProbe(IFrameWriter writer) throws HyracksDataException {
        //We do NOT join the spilled partitions here, that decision is made at the descriptor level
        //(which join technique to use)

        if (spilled) {
            table.printInfo();
            runFileStreamForProbe.flushRunFile();
            runFileStreamForBuild.flushRunFile();
            //runFileStreamForProbe.startReadingRunFile(inputCursor[BUILD_PARTITION]);
        }
        table.printInfo();
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