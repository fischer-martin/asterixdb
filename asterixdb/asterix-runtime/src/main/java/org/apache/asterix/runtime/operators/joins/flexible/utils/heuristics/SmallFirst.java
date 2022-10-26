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

public class SmallFirst implements IHeuristicForThetaJoin {
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

    boolean roleReversal = false;

    public SmallFirst(int memoryForJoin, int frameSize, long buildFileSize, long probeFileSize, RecordDescriptor buildRd, RecordDescriptor probeRd, boolean checkForRoleReversal)
            throws HyracksDataException {
        this.memoryForJoinInBytes = memoryForJoin * frameSize;
        this.memoryForJoinInFrames = memoryForJoin;
        this.frameSize = frameSize;
        this.buildFileSize = buildFileSize;
        this.probeFileSize = probeFileSize;
        this.hasNextBuildingBucketSequence = true;
        this.buildRd = buildRd;
        this.probeRd = probeRd;
        //this.roleReversal = true;
        if(checkForRoleReversal && probeFileSize < buildFileSize) this.roleReversal = true;
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
        for (int[] bucket : bucketsFromR) {

            int bucketSize = bucket[1];

            int endFrame = bucket[4];
            int endOffset = bucket[5];
//            if (Math.ceil(((double) totalSizeForBuckets + bucketSize) * CONSTANT / frameSize) <= memoryForJoinInFrames) {
//                totalSizeForBuckets += bucketSize;
//                removeList.add(bucket);
//                Bucket returnBucket;
//                returnBucket = new Bucket(bucket[0], roleReversal?1:0, bucket[3], endOffset,
//                        bucket[2], endFrame);
//                returnBuckets.add(returnBucket);
//            }

            if (Math.ceil(((double) totalSizeForBuckets + bucketSize) * CONSTANT / frameSize) <= memoryForJoinInFrames) {
                totalSizeForBuckets += bucketSize;

            } else break;
            removeList.add(bucket);
            Bucket returnBucket;
            returnBucket = new Bucket(bucket[0], roleReversal?1:0, bucket[3], endOffset,
                    bucket[2], endFrame);
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
            if(!roleReversal) {
                if (bucket[0] == -1 || bucket[1] > -1 || bucket[2] == -1) {
                    continue;
                }
            } else {
                if (bucket[0] == -1 || bucket[3] > -1 || bucket[4] == -1) {
                    continue;
                }
            }
            tempBucketsFromR.add(bucket);
        }
        tempBucketsFromR.sort(Comparator.comparingDouble(o -> -o[roleReversal?3:1]));

        for (int i = 0; i < tempBucketsFromR.size(); i++) {
            int[] bucket = tempBucketsFromR.get(i);
            int bucketSize;
            int startOffsetInFile;
            int startFrame;
            int startOffset;
            if(!roleReversal) {
                startOffsetInFile = -((bucket[1] + 1) * this.frameSize) + bucket[2];
                startFrame = -(bucket[1]+1);
                startOffset = bucket[2];
            } else {
                startOffsetInFile = -((bucket[3] + 1) * this.frameSize) + bucket[4];
                startFrame = -(bucket[3] + 1);
                startOffset = bucket[4];
            }
            int[] nextBucket = new int[5];
            int endFrame;
            int endOffset;
            if (i + 1 < tempBucketsFromR.size()) {
                int nextOnDisk;
                for(nextOnDisk = i + 1; nextOnDisk < tempBucketsFromR.size(); nextOnDisk++) {
                    nextBucket = tempBucketsFromR.get(nextOnDisk);
                    if(!roleReversal) {
                        if (nextBucket[1] < 0) break;
                    } else {
                        if (nextBucket[3] < 0) break;
                    }
                }
                if(!roleReversal) {
                    endFrame = -(nextBucket[1] + 1);
                    endOffset = nextBucket[2];
                } else {
                    endFrame = -(nextBucket[3] + 1);
                    endOffset = nextBucket[4];
                }
                bucketSize = ((endFrame * this.frameSize) + endOffset) - startOffsetInFile;

            } else {
                endFrame = -1;
                endOffset = -1;
                if(!roleReversal) {
                    bucketSize = (int) ((buildFileSize + 5) - startOffsetInFile);
                } else {
                    bucketSize = (int) ((probeFileSize + 5) - startOffsetInFile);
                }
            }
            //This part is implemented by assuming every bucket will start from a new frame
            if(bucketSize > memoryForJoinInBytes) {
                int tempBucketSize = bucketSize;
                while(tempBucketSize > 0) {
                    int currentBucketSize = Math.min(memoryForJoinInBytes, tempBucketSize);
                    int[] newBucket = new int[6];
                    newBucket[0] = bucket[0];
                    newBucket[1] = currentBucketSize;
                    newBucket[2] = startFrame;
                    newBucket[3] = 5;
                    newBucket[4] = startFrame + (currentBucketSize / frameSize);
                    newBucket[5] = 5;
                    this.bucketsFromR.add(newBucket);
                    startFrame += (currentBucketSize/frameSize);
                    tempBucketSize -= memoryForJoinInBytes;
                }
            } else {
                int[] newBucket = new int[6];
                newBucket[0] = bucket[0];
                newBucket[1] = bucketSize;
                newBucket[2] = startFrame;
                newBucket[3] = startOffset;
                newBucket[4] = endFrame;
                newBucket[5] = endOffset;
                this.bucketsFromR.add(newBucket);
            }
        }
        bucketsFromR.sort(Comparator.comparingDouble(o -> o[1]));
    }

    @Override
    public void setComparator(ITuplePairComparator comparator) {

    }
}
