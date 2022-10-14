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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.asterix.runtime.operators.joins.flexible.utils.Bucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IBucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IHeuristicForThetaJoin;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.structures.SerializableBucketIdList;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class BigFirst implements IHeuristicForThetaJoin {
    double CONSTANT = 1.5;
    SerializableBucketIdList bucketTable;
    long buildFileSize;
    long probeFileSize;
    int memoryForJoinInBytes;
    int memoryForJoinInFrames;
    int frameSize;
    boolean hasNextBuildingBucketSequence;
    int numberOfBuckets;
    int buildingBucketPosition = 0;

    long[][] bucketsFromR;

    public BigFirst(int memoryForJoin, int frameSize, long buildFileSize, long probeFileSize)
            throws HyracksDataException {
        this.memoryForJoinInBytes = memoryForJoin * frameSize;
        this.memoryForJoinInFrames = memoryForJoin;
        this.frameSize = frameSize;
        this.buildFileSize = buildFileSize;
        this.probeFileSize = probeFileSize;
        this.hasNextBuildingBucketSequence = true;

    }

    @Override
    public boolean hasNextBuildingBucketSequence() {
        for (int i = 0; i < this.numberOfBuckets; i++) {
            int[] bucket = bucketTable.getEntry(i);
            if (bucket[1] < 0)
                return true;
        }
        return false;
    }

    @Override
    public ArrayList<IBucket> nextBuildingBucketSequence() throws HyracksDataException {
        ArrayList<IBucket> returnBuckets = new ArrayList<>();
        int totalFramesForBuckets = 0;
        long totalSizeForBuckets = 0;
        int currentFrame = 0;
        for (int idx = 0; idx < this.numberOfBuckets; idx++) {
            long[] bucket = bucketsFromR[idx];

            int i = (int) bucket[2];
            int[] bucketInfo = bucketTable.getEntry(i);
            if (bucket[1] == 0 || bucket[1] < 0)
                continue;
            if (bucketInfo[0] == -1) {
                this.hasNextBuildingBucketSequence = false;
                return returnBuckets;
            }
            long bucketSize;
            long startOffsetInFile = -(((bucketInfo[1] + 1) * this.frameSize) + bucketInfo[2]);
            int endFrame;
            int endOffset;
            if (i + 1 < this.numberOfBuckets) {
                bucketSize = -((long) (bucketTable.getEntry(i + 1)[1] + 1) * this.frameSize)
                        + bucketTable.getEntry(i + 1)[2] - startOffsetInFile;
                endFrame = -(bucketTable.getEntry(i + 1)[1] + 1);
                endOffset = bucketTable.getEntry(i + 1)[2];
            } else {
                bucketSize = buildFileSize - startOffsetInFile;
                endFrame = -1;
                endOffset = -1;
            }

            if (Math.ceil(((double) totalSizeForBuckets + bucket[1]) * CONSTANT / frameSize) < memoryForJoinInFrames)
                totalSizeForBuckets += bucket[1];
            else
                break;

            bucketTable.updateBuildBucket(bucketInfo[0], new TuplePointer(-1, -1));
            Bucket returnBucket =
                    new Bucket(bucketInfo[0], 0, bucketInfo[2], endOffset, -(bucketInfo[1] + 1), endFrame);
            returnBuckets.add(returnBucket);
        }

        return returnBuckets;
    }

    @Override
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
        this.bucketsFromR = new long[numberOfBuckets][4];
        for (int i = 0; i < this.numberOfBuckets; i++) {
            int[] bucket = bucketTable.getEntry(i);
            if (bucket[0] == -1 || bucket[1] > -1 || bucket[2] == -1) {
                continue;
            }
            long bucketSize;
            long startOffsetInFile = -((long) (bucket[1] + 1) * this.frameSize) + bucket[2];
            if (i + 1 < this.numberOfBuckets) {
                bucketSize = -((long) (bucketTable.getEntry(i + 1)[1] + 1) * this.frameSize)
                        + bucketTable.getEntry(i + 1)[2] - startOffsetInFile;
            } else {
                bucketSize = buildFileSize - startOffsetInFile;
            }
            bucketsFromR[i][0] = bucket[0];
            bucketsFromR[i][1] = bucketSize;
            bucketsFromR[i][2] = i;

        }
        Arrays.sort(bucketsFromR, Comparator.comparingDouble(o -> -o[1]));
    }
}
