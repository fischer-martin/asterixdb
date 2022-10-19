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
package org.apache.asterix.runtime.operators.joins.flexible.utils.heuristics;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.asterix.runtime.operators.joins.flexible.utils.Bucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IBucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IHeuristicForThetaJoin;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.structures.SerializableBucketIdList;

public class FirstFit implements IHeuristicForThetaJoin {
    double CONSTANT = 1;
    SerializableBucketIdList bucketTable;
    long buildFileSize;
    long probeFileSize;
    int memoryForJoinInBytes;
    int memoryForJoinInFrames;
    int frameSize;
    boolean hasNextBuildingBucketSequence;
    int numberOfBuckets;
    int buildingBucketPosition = 0;

    ArrayList<int[]> bucketsFromR;
    ArrayList<int[]> tempBucketsFromR;

    RecordDescriptor buildRd;
    RecordDescriptor probeRd;

    public FirstFit(int memoryForJoin, int frameSize, long buildFileSize, long probeFileSize, RecordDescriptor buildRd, RecordDescriptor probeRd)
            throws HyracksDataException {
        this.memoryForJoinInBytes = memoryForJoin * frameSize;
        this.memoryForJoinInFrames = memoryForJoin;
        this.frameSize = frameSize;
        this.buildFileSize = buildFileSize;
        this.probeFileSize = probeFileSize;
        this.hasNextBuildingBucketSequence = true;
        this.buildRd = buildRd;
        this.probeRd = probeRd;

    }

    @Override
    public boolean hasNextBuildingBucketSequence() {
        return !bucketsFromR.isEmpty();
    }

    @Override
    public ArrayList<IBucket> nextBuildingBucketSequence() throws HyracksDataException {
        ArrayList<IBucket> returnBuckets = new ArrayList<>();
        int totalFramesForBuckets = 0;
        long totalSizeForBuckets = 0;
        int currentFrame = 0;
        ArrayList<int[]> removeList = new ArrayList<>();
        for (int i = 0; i < bucketsFromR.size(); i++) {
            int[] bucket = bucketsFromR.get(i);
            long entryInTable = bucket[4];
            int[] bucketInfoFromTable = tempBucketsFromR.get((int) entryInTable);

            int bucketSize = bucket[1];
            int endFrame = bucket[2];
            int endOffset = bucket[3];

            if (Math.ceil(((double) totalSizeForBuckets + bucketSize) * CONSTANT / frameSize) < memoryForJoinInFrames)
                totalSizeForBuckets += bucketSize;
            else
                break;

            removeList.add(bucket);
            Bucket returnBucket = new Bucket(bucketInfoFromTable[0], 0, bucketInfoFromTable[2], endOffset,
                    -(bucketInfoFromTable[1] + 1), endFrame);
            returnBuckets.add(returnBucket);
        }
        bucketsFromR.removeAll(removeList);
        return returnBuckets;
    }

    public ArrayList<IBucket> nextProbingBucketSequence() {
        ArrayList<IBucket> returnBuckets = new ArrayList<>();
        int totalSizeOfBuckets = 0;

        for (int i = 0; i < this.numberOfBuckets; i++) {
            int[] bucket = bucketTable.getEntry(i);

            if (bucket[0] == -1 || bucket[4] == -1) {
                continue;
            }
            if (bucket[3] >= 0)
                continue;
            long bucketSize;
            long startOffsetInFile = -((long) (bucket[3] + 1) * this.frameSize) + bucket[4];
            int endFrame;
            int endOffset;
            if (i + 1 < this.numberOfBuckets) {
                bucketSize = -((long) (bucketTable.getEntry(i + 1)[3] + 1) * this.frameSize)
                        + bucketTable.getEntry(i + 1)[4] - startOffsetInFile;
                endOffset = bucketTable.getEntry(i + 1)[4];
                endFrame = -(bucketTable.getEntry(i + 1)[3] + 1);
            } else {
                bucketSize = probeFileSize - startOffsetInFile;
                endOffset = -1;
                endFrame = -1;
            }

            //totalSizeOfBuckets += bucketSize;
            //if(totalSizeOfBuckets > memoryForJoinInBytes) {
            //    break;
            //}
            //bucketTable.updateBuildBucket(bucket[0], new TuplePointer(0,0));
            Bucket returnBucket = new Bucket(bucket[0], 1, bucket[4], endOffset, -(bucket[3] + 1), endFrame);
            returnBuckets.add(returnBucket);
        }

        return returnBuckets;
    }

    public void setBucketTable(SerializableBucketIdList bucketTable) {
        this.bucketTable = bucketTable;
        this.numberOfBuckets = bucketTable.getNumEntries();
        this.bucketsFromR = new ArrayList<>();
        tempBucketsFromR = new ArrayList<>();
        for(int i = 0; i < this.numberOfBuckets; i++) {
            int[] bucket = bucketTable.getEntry(i);
            if (bucket[0] == -1 || bucket[1] > -1 || bucket[2] == -1) {
                continue;
            }
            tempBucketsFromR.add(bucket);
        }
        tempBucketsFromR.sort(Comparator.comparingDouble(o -> -o[1]));
        for (int i = 0; i < tempBucketsFromR.size(); i++) {
            int[] bucket = tempBucketsFromR.get(i);
            int bucketSize;
            int startOffsetInFile = -((bucket[1] + 1) * this.frameSize) + bucket[2];
            int[] nextBucket = new int[5];
            int endFrame;
            int endOffset;
            if (i + 1 < tempBucketsFromR.size()) {
                int nextOnDisk;
                for(nextOnDisk = i + 1; nextOnDisk < tempBucketsFromR.size(); nextOnDisk++) {
                    nextBucket = tempBucketsFromR.get(nextOnDisk);
                    if(nextBucket[1] < 0) break;
                }
                endFrame = -(nextBucket[1] + 1);
                endOffset = nextBucket[2];
                bucketSize = ((endFrame * this.frameSize) + endOffset) - startOffsetInFile;

            } else {
                endFrame = -1;
                endOffset = -1;
                bucketSize = (int) ((buildFileSize + 5) - startOffsetInFile);
            }
            int[] newBucket = new int[5];
            newBucket[0] = bucket[0];
            newBucket[1] = bucketSize;
            newBucket[2] = endFrame;
            newBucket[3] = endOffset;
            newBucket[4] = i;
            this.bucketsFromR.add(newBucket);
        }
        //bucketsFromR.sort(Comparator.comparingDouble(o -> -o[1]));
    }

    @Override
    public void setComparator(ITuplePairComparator comparator) {

    }
}
