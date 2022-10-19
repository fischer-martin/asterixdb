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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.asterix.runtime.operators.joins.flexible.utils.Bucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IBucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IHeuristicForThetaJoin;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.structures.SerializableBucketIdList;

public class Weighted implements IHeuristicForThetaJoin {
    double CONSTANT = 1;
    double SEEK_PENALTY = 1000;
    SerializableBucketIdList bucketTable;
    long buildFileSize;
    long probeFileSize;
    int memoryForJoinInBytes;
    int memoryForJoinInFrames;
    int frameSize;
    boolean hasNextBuildingBucketSequence;
    int numberOfBuckets;
    int buildingBucketPosition = 0;
    ITuplePairComparator comparator;

    ArrayList<int[]> bucketsFromR;
    ArrayList<int[]> bucketsFromS;
    ArrayList<int[]> tempBucketsFromR;
    ArrayList<int[]> tempBucketsFromS;

    RecordDescriptor buildRd;
    RecordDescriptor probeRd;

    public Weighted(int memoryForJoin, int frameSize, long buildFileSize, long probeFileSize, RecordDescriptor buildRd, RecordDescriptor probeRd)
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
        return !bucketsFromR.isEmpty() || !bucketsFromS.isEmpty();
    }

    @Override
    public ArrayList<IBucket> nextBuildingBucketSequence() throws HyracksDataException {
        ArrayList<IBucket> returnBucketsR = new ArrayList<>();
        ArrayList<IBucket> returnBucketsS = new ArrayList<>();
        int totalFramesForBuckets = 0;
        long totalSizeForBuckets = 0;
        int currentFrame = 0;
        ArrayList<int[]> removeListR = new ArrayList<>();
        ArrayList<int[]> removeListS = new ArrayList<>();
        int costR = 0;
        int costS = 0;
        for (int i = 0; i < bucketsFromR.size(); i++) {
            int[] bucket = bucketsFromR.get(i);
            long entryInTable = bucket[4];
            int[] bucketInfoFromTable = tempBucketsFromR.get((int) entryInTable);

            int bucketSize = bucket[1];
            int endFrame = bucket[2];
            int endOffset = bucket[3];

            if (Math.ceil(((double) totalSizeForBuckets + bucketSize) * CONSTANT / frameSize) < memoryForJoinInFrames) {
                totalSizeForBuckets += bucketSize;
                costR += bucket[5];
            }
            else
                break;

            removeListR.add(bucket);
            Bucket returnBucket = new Bucket(bucketInfoFromTable[0], 0, bucketInfoFromTable[2], endOffset,
                    -(bucketInfoFromTable[1] + 1), endFrame);
            returnBucketsR.add(returnBucket);
        }
        totalSizeForBuckets = 0;
        for (int i = 0; i < bucketsFromS.size(); i++) {
            int[] bucket = bucketsFromS.get(i);
            long entryInTable = bucket[4];
            int[] bucketInfoFromTable = tempBucketsFromS.get((int) entryInTable);

            int bucketSize = bucket[1];
            int endFrame = bucket[2];
            int endOffset = bucket[3];

            if (Math.ceil(((double) totalSizeForBuckets + bucketSize) * CONSTANT / frameSize) < memoryForJoinInFrames) {
                totalSizeForBuckets += bucketSize;
                costS += bucket[5];
            }
            else
                break;

            removeListS.add(bucket);
            Bucket returnBucket = new Bucket(bucketInfoFromTable[0], 1, bucketInfoFromTable[4], endOffset,
                    -(bucketInfoFromTable[3] + 1), endFrame);
            returnBucketsS.add(returnBucket);
        }
        if(costR < costS && returnBucketsS.size() > 0) {
            bucketsFromS.removeAll(removeListS);
            return returnBucketsS;
        } else if(returnBucketsR.size() > 0) {
            bucketsFromR.removeAll(removeListR);
            return returnBucketsR;
        } else return null;
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

    public void setBucketTable(SerializableBucketIdList bucketTable) throws HyracksDataException {
        this.bucketTable = bucketTable;
        this.numberOfBuckets = bucketTable.getNumEntries();
        this.bucketsFromR = new ArrayList<>();
        this.bucketsFromS = new ArrayList<>();

        tempBucketsFromR = new ArrayList<>();
        for(int i = 0; i < this.numberOfBuckets; i++) {
            int[] bucket = bucketTable.getEntry(i);
            if (bucket[0] == -1 || bucket[1] > -1 || bucket[2] == -1) {
                continue;
            }
            tempBucketsFromR.add(bucket);
        }
        tempBucketsFromR.sort(Comparator.comparingDouble(o -> -o[1]));

        tempBucketsFromS = new ArrayList<>();
        for(int i = 0; i < this.numberOfBuckets; i++) {
            int[] bucket = bucketTable.getEntry(i);
            if (bucket[0] == -1 || bucket[3] > -1 || bucket[4] == -1) {
                continue;
            }
            tempBucketsFromS.add(bucket);
        }
        tempBucketsFromS.sort(Comparator.comparingDouble(o -> -o[1]));

        ArrayList<int[]> tempTwoR = new ArrayList<>();
        ArrayList<int[]> tempTwoS = new ArrayList<>();
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
            tempTwoR.add(newBucket);
        }

        for (int i = 0; i < tempBucketsFromS.size(); i++) {
            int[] bucket = tempBucketsFromS.get(i);
            int bucketSize;
            int startOffsetInFile = -((bucket[3] + 1) * this.frameSize) + bucket[4];
            int[] nextBucket = new int[5];
            int endFrame;
            int endOffset;
            if (i + 1 < tempBucketsFromS.size()) {
                int nextOnDisk;
                for(nextOnDisk = i + 1; nextOnDisk < tempBucketsFromS.size(); nextOnDisk++) {
                    nextBucket = tempBucketsFromS.get(nextOnDisk);
                    if(nextBucket[3] < 0) break;
                }
                endFrame = -(nextBucket[3] + 1);
                endOffset = nextBucket[4];
                bucketSize = ((endFrame * this.frameSize) + endOffset) - startOffsetInFile;

            } else {
                endFrame = -1;
                endOffset = -1;
                bucketSize = (int) ((probeFileSize + 5) - startOffsetInFile);
            }
            int[] newBucket = new int[5];
            newBucket[0] = bucket[0];
            newBucket[1] = bucketSize;
            newBucket[2] = endFrame;
            newBucket[3] = endOffset;
            newBucket[4] = i;
            tempTwoS.add(newBucket);
        }

        IFrameTupleAccessor iFrameTupleAccessorForR =
                new FrameTupleAccessor(new RecordDescriptor(Arrays.copyOfRange(this.buildRd.getFields(), 0, 2),
                        Arrays.copyOfRange(buildRd.getTypeTraits(), 0, 1)));
        byte[] byteArrayForR = new byte[21];
        ByteBuffer buffForR = ByteBuffer.wrap(byteArrayForR);

        IFrameTupleAccessor iFrameTupleAccessorForS =
                new FrameTupleAccessor(new RecordDescriptor(Arrays.copyOfRange(this.probeRd.getFields(), 0, 2),
                        Arrays.copyOfRange(probeRd.getTypeTraits(), 0, 1)));
        byte[] byteArrayForS = new byte[21];
        ByteBuffer buffForS = ByteBuffer.wrap(byteArrayForS);

        for(int i = 0; i < tempTwoR.size(); i++) {
            int costR = tempTwoR.get(i)[1];
            int costS = 0;
            int prev = 0;

            buffForR.position(14);
            buffForR.putInt(tempTwoR.get(i)[0]);
            iFrameTupleAccessorForR.reset(buffForR);

            for(int j = 0; j < tempTwoS.size(); j++) {

                buffForS.position(14);
                buffForS.putInt(tempTwoS.get(j)[0]);
                iFrameTupleAccessorForS.reset(buffForS);

                if(comparator.compare(iFrameTupleAccessorForR, 0, iFrameTupleAccessorForS, 0) < 1) {
                    costS += tempTwoS.get(j)[1];
                    if(prev + 1 != j) {
                        costS += SEEK_PENALTY;
                    }
                }
            }

            int[] newBucket = new int[6];
            newBucket[0] = tempTwoR.get(i)[0];
            newBucket[1] = tempTwoR.get(i)[1];
            newBucket[2] = tempTwoR.get(i)[2];
            newBucket[3] = tempTwoR.get(i)[3];
            newBucket[4] = tempTwoR.get(i)[4];
            newBucket[5] = costS / costR;
            bucketsFromR.add(newBucket);

        }
        bucketsFromR.sort(Comparator.comparingDouble(o -> -o[5]));

        for(int i = 0; i < tempTwoS.size(); i++) {
            int costS = tempTwoS.get(i)[1];
            int costR = 0;
            int prev = 0;

            buffForS.position(14);
            buffForS.putInt(tempTwoS.get(i)[0]);
            iFrameTupleAccessorForS.reset(buffForS);

            for(int j = 0; j < tempTwoR.size(); j++) {

                buffForR.position(14);
                buffForR.putInt(tempTwoR.get(j)[0]);
                iFrameTupleAccessorForR.reset(buffForR);

                if(comparator.compare(iFrameTupleAccessorForR, 0, iFrameTupleAccessorForS, 0) < 1) {
                    costR += tempTwoR.get(j)[1];
                    if(prev + 1 != j) {
                        costR += SEEK_PENALTY;
                    }
                }
            }

            int[] newBucket = new int[6];
            newBucket[0] = tempTwoS.get(i)[0];
            newBucket[1] = tempTwoS.get(i)[1];
            newBucket[2] = tempTwoS.get(i)[2];
            newBucket[3] = tempTwoS.get(i)[3];
            newBucket[4] = tempTwoS.get(i)[4];
            newBucket[5] = costR / costS;
            bucketsFromS.add(newBucket);

        }
        bucketsFromS.sort(Comparator.comparingDouble(o -> -o[5]));
        //System.out.println("test");
    }
    @Override
    public void setComparator(ITuplePairComparator comparator) {
        this.comparator = comparator;
    }
}
