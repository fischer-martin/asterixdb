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
import java.util.HashMap;
import java.util.List;

import org.apache.asterix.runtime.operators.joins.flexible.utils.memory.FlexibleJoinsUtil;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.TupleInFrameListAccessor;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InMemoryFlexibleJoin {

    private final List<ByteBuffer> buffers;
    private final FrameTupleAccessor accessorBuild;
    private final ITuplePartitionComputer tpcBuild;
    private final IFrameTupleAccessor accessorProbe;
    private final ITuplePartitionComputer tpcProbe;
    private final FrameTupleAppender appender;
    private ITuplePairComparator tpComparator;
    private final SerializableHashTable table;
    private final TuplePointer storedTuplePointer;
    private final boolean reverseOutputOrder; //Should we reverse the order of tuples, we are writing in output
    private final IPredicateEvaluator predEvaluator;
    private final TupleInFrameListAccessor tupleAccessor;
    // To release frames
    private final ISimpleFrameBufferManager bufferManager;
    private final boolean isTableCapacityNotZero;

    private int probeCounter = 0;

    private HashMap<Integer, ArrayList<Integer>> binMap;

    private static final Logger LOGGER = LogManager.getLogger();

    public InMemoryFlexibleJoin(IHyracksFrameMgrContext ctx, FrameTupleAccessor accessorProbe,
            ITuplePartitionComputer tpcProbe, FrameTupleAccessor accessorBuild, RecordDescriptor rDBuild,
            ITuplePartitionComputer tpcBuild, SerializableHashTable table, IPredicateEvaluator predEval,
            ISimpleFrameBufferManager bufferManager) throws HyracksDataException {
        this(ctx, accessorProbe, tpcProbe, accessorBuild, rDBuild, tpcBuild, table, predEval, false, bufferManager);
    }

    public InMemoryFlexibleJoin(IHyracksFrameMgrContext ctx, FrameTupleAccessor accessorProbe,
            ITuplePartitionComputer tpcProbe, FrameTupleAccessor accessorBuild, RecordDescriptor rDBuild,
            ITuplePartitionComputer tpcBuild, SerializableHashTable table, IPredicateEvaluator predEval,
            boolean reverse, ISimpleFrameBufferManager bufferManager) throws HyracksDataException {
        this.table = table;
        storedTuplePointer = new TuplePointer();
        buffers = new ArrayList<>();
        this.accessorBuild = accessorBuild;
        this.tpcBuild = tpcBuild;
        this.accessorProbe = accessorProbe;
        this.tpcProbe = tpcProbe;
        appender = new FrameTupleAppender(new VSizeFrame(ctx));
        predEvaluator = predEval;

        reverseOutputOrder = reverse;
        this.tupleAccessor = new TupleInFrameListAccessor(rDBuild, buffers);
        this.bufferManager = bufferManager;
        this.isTableCapacityNotZero = table.getTableSize() != 0;
        this.binMap = new HashMap<>();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("InMemoryHashJoin has been created for a table size of " + table.getTableSize()
                    + " for Thread ID " + Thread.currentThread().getId() + ".");
        }
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        accessorBuild.reset(buffer);
        int tCount = accessorBuild.getTupleCount();
        if (tCount <= 0) {
            return;
        }
        buffers.add(buffer);
        int bIndex = buffers.size() - 1;
        for (int i = 0; i < tCount; ++i) {
            int entry = tpcBuild.partition(accessorBuild, i, table.getTableSize());
            storedTuplePointer.reset(bIndex, i);
            // If an insertion fails, then tries to insert the same tuple pointer again after compacting the table.
            //int bId = FlexibleJoinsUtil.getBucketId(accessorBuild, i, 1);
            //System.out.println(bId+"\t"+pId);
            //System.out.println("entry:"+entry+"\t"+bId);
            if (!table.insert(entry, storedTuplePointer)) {
                if (!compactTableAndInsertAgain(entry, storedTuplePointer)) {
                    throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                            "Record insertion failed in in-memory hash join even after compaction.");
                }
            }
        }
    }

    public boolean compactTableAndInsertAgain(int entry, TuplePointer tPointer) throws HyracksDataException {
        boolean oneMoreTry = false;
        if (compactHashTable() >= 0) {
            oneMoreTry = table.insert(entry, tPointer);
        }
        return oneMoreTry;
    }

    /**
     * Tries to compact the table to make some space.
     *
     * @return the number of frames that have been reclaimed. If no compaction has happened, the value -1 is returned.
     */
    public int compactHashTable() throws HyracksDataException {
        if (table.isGarbageCollectionNeeded()) {
            return table.collectGarbage(tupleAccessor, tpcBuild);
        }
        return -1;
    }

    /**
     * Must be called before starting to join to set the right comparator with the right context.
     *
     * @param comparator the comparator to use for comparing the probe tuples against the build tuples
     */
    void setComparator(ITuplePairComparator comparator) {
        tpComparator = comparator;
    }

    /**
     * Reads the given tuple from the probe side and joins it with tuples from the build side.
     * This method assumes that the accessorProbe is already set to the current probe frame.
     */
    void join(int tid, IFrameWriter writer) throws HyracksDataException {
        if (isTableCapacityNotZero) {
            //int entry = tpcBuild.partition(accessorProbe, tid, table.getTableSize());
            int pId = FlexibleJoinsUtil.getBucketId(accessorProbe, tid, 1);
            //if(tpComparator.equals(Comp))

            //int[] entrySet = ArrayUtils.toPrimitive(table.getKeys().toArray());
            //ArrayList<Integer> entryList;

            for (int currentEntry : table.getKeys()) {
                int tupleCount = table.getTupleCount(currentEntry);

                for (int i = 0; i < tupleCount; i++) {
                    table.getTuplePointer(currentEntry, i, storedTuplePointer);
                    int bIndex = storedTuplePointer.getFrameIndex();
                    int tIndex = storedTuplePointer.getTupleIndex();
                    accessorBuild.reset(buffers.get(bIndex));
                    int c = tpComparator.compare(accessorProbe, tid, accessorBuild, tIndex);
                    int bId = FlexibleJoinsUtil.getBucketId(accessorBuild, tIndex, 1);
                    System.out.println("entry:" + currentEntry + "\t" + bId);
                    //System.out.println(bId+"\t"+pId);
                    //System.out.println("entry:"+entry+"\t"+bId+"\t"+pId);
                    /*if (c == 0) {
                        boolean predEval = evaluatePredicate(tid, tIndex);
                        //System.out.println("True:"+bId+"\t"+pId);
                        if (predEval) {
                            //if(currentEntry != entry) System.out.println("Entry:"+entry+"\tcurrentEntry:"+currentEntry);
                            appendToResult(tid, tIndex, writer);
                        }
                    } else {
                        System.out.println("Break");
                        break;
                    }*/
                }
                /*if(!foundOne) {
                    //System.out.println("Entry:"+entry+"\tcurrentEntry:"+currentEntry);
                    //toRemove.add(currentEntry);
                }*/

            }
            //entryList.removeAll(toRemove);
            //toRemove.clear();
            //binMap.putIfAbsent(entry, entryList);
        }
    }

    public void join(ByteBuffer buffer, IFrameWriter writer, int partition) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount0 = accessorProbe.getTupleCount();
        for (int i = 0; i < tupleCount0; ++i) {
            int buid = FlexibleJoinsUtil.getBucketId(accessorProbe, i, 1);
            //System.out.println("Partition:"+partition+"\t"+"pid:"+buid);
            join(i, writer);
        }
    }

    public void resetAccessorProbe(IFrameTupleAccessor newAccessorProbe) {
        accessorProbe.reset(newAccessorProbe.getBuffer());
    }

    public void completeJoin(IFrameWriter writer) throws HyracksDataException {
        appender.write(writer, true);
    }

    public void releaseMemory() throws HyracksDataException {
        int nFrames = buffers.size();
        // Frames assigned to the data table will be released here.
        if (bufferManager != null) {
            for (int i = 0; i < nFrames; i++) {
                bufferManager.releaseFrame(buffers.get(i));
            }
        }
        buffers.clear();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("InMemoryHashJoin has finished using " + nFrames + " frames for Thread ID "
                    + Thread.currentThread().getId() + ".");
        }
    }

    public void closeTable() throws HyracksDataException {
        //System.out.println(table.printInfo());
        table.close();
    }

    private void appendToResult(int probeSidetIx, int buildSidetIx, IFrameWriter writer) throws HyracksDataException {
        if (reverseOutputOrder) {
            FrameUtils.appendConcatToWriter(writer, appender, accessorBuild, buildSidetIx, accessorProbe, probeSidetIx);
        } else {
            FrameUtils.appendConcatToWriter(writer, appender, accessorProbe, probeSidetIx, accessorBuild, buildSidetIx);
        }
    }
}
