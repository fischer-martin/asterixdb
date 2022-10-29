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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.asterix.om.types.ATypeTag;
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

    ArrayList<int[]> probingBucketSequence;
    ArrayList<IBucket> returnedBuildingBucketSequence;

    boolean roleReversal = false;
    boolean checkForRoleReversal = false;

    public Weighted(int memoryForJoin, int frameSize, long buildFileSize, long probeFileSize, RecordDescriptor buildRd,
            RecordDescriptor probeRd, boolean checkForRoleReversal) throws HyracksDataException {
        this.memoryForJoinInBytes = memoryForJoin * frameSize;
        this.memoryForJoinInFrames = memoryForJoin;
        this.frameSize = frameSize;
        this.buildFileSize = buildFileSize;
        this.probeFileSize = probeFileSize;
        this.hasNextBuildingBucketSequence = true;
        this.buildRd = buildRd;
        this.probeRd = probeRd;
        this.checkForRoleReversal = checkForRoleReversal;
        //if(checkForRoleReversal && probeFileSize < buildFileSize) this.roleReversal = true;
    }

    @Override
    public boolean hasNextBuildingBucketSequence() {
        return !bucketsFromR.isEmpty() && !bucketsFromS.isEmpty();
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

            int bucketSize = bucket[1];
            int startFrame = bucket[2];
            int startOffset = bucket[3];

            int endFrame = bucket[4];
            int endOffset = bucket[5];

            if (Math.ceil(
                    ((double) totalSizeForBuckets + bucketSize) * CONSTANT / frameSize) <= memoryForJoinInFrames) {
                totalSizeForBuckets += bucketSize;
                costR += bucket[6];
                removeListR.add(bucket);
                Bucket returnBucket = new Bucket(bucket[0], 0, startOffset, endOffset, startFrame, endFrame);
                returnBucketsR.add(returnBucket);
            }
        }
        totalSizeForBuckets = 0;
        for (int i = 0; i < bucketsFromS.size(); i++) {
            int[] bucket = bucketsFromS.get(i);

            int bucketSize = bucket[1];
            int startFrame = bucket[2];
            int startOffset = bucket[3];

            int endFrame = bucket[4];
            int endOffset = bucket[5];

            if (Math.ceil(
                    ((double) totalSizeForBuckets + bucketSize) * CONSTANT / frameSize) <= memoryForJoinInFrames) {
                totalSizeForBuckets += bucketSize;
                costS += bucket[6];
                removeListS.add(bucket);
                Bucket returnBucket = new Bucket(bucket[0], 1, startOffset, endOffset, startFrame, endFrame);
                returnBucketsS.add(returnBucket);
            }

        }
        if (checkForRoleReversal && costS > costR) {
            bucketsFromS.removeAll(removeListS);
            probingBucketSequence = bucketsFromR;
            roleReversal = true;
            returnedBuildingBucketSequence = returnBucketsS;
            return returnBucketsS;
        } else {
            roleReversal = false;
            bucketsFromR.removeAll(removeListR);
            probingBucketSequence = bucketsFromS;
            returnedBuildingBucketSequence = returnBucketsR;
            return returnBucketsR;
        }
        //        bucketsFromS.removeAll(removeListS);
        //        return returnBucketsS;
    }

    public ArrayList<IBucket> nextProbingBucketSequence() throws HyracksDataException {
        ArrayList<IBucket> returnBuckets = new ArrayList<>();
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

        for (int[] bucket : probingBucketSequence) {

            int startFrame = bucket[2];
            int starOffset = bucket[3];;

            int endFrame = bucket[4];
            int endOffset = bucket[5];

            if (!roleReversal) {
                buffForS.position(13);
                buffForS.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
                buffForS.putInt(bucket[0]);
                iFrameTupleAccessorForS.reset(buffForS);

                for (IBucket rBucket : returnedBuildingBucketSequence) {

                    buffForR.position(13);
                    buffForR.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
                    buffForR.putInt(rBucket.getBucketId());
                    iFrameTupleAccessorForR.reset(buffForR);

                    if (comparator.compare(iFrameTupleAccessorForR, 0, iFrameTupleAccessorForS, 0) < 1) {
                        Bucket returnBucket = new Bucket(bucket[0], 1, starOffset, endOffset, startFrame, endFrame);
                        returnBuckets.add(returnBucket);
                    }
                }
            } else {
                buffForR.position(13);
                buffForR.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
                buffForR.putInt(bucket[0]);
                iFrameTupleAccessorForS.reset(buffForR);

                for (IBucket sBucket : returnedBuildingBucketSequence) {

                    buffForS.position(13);
                    buffForS.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
                    buffForS.putInt(sBucket.getBucketId());
                    iFrameTupleAccessorForS.reset(buffForS);

                    if (comparator.compare(iFrameTupleAccessorForR, 0, iFrameTupleAccessorForS, 0) < 1) {
                        Bucket returnBucket = new Bucket(bucket[0], 0, starOffset, endOffset, startFrame, endFrame);
                        returnBuckets.add(returnBucket);
                    }
                }
            }

        }
        reComputeCosts();
        return returnBuckets;
    }

    public void reComputeCosts() throws HyracksDataException {

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
        ArrayList<int[]> tempList = new ArrayList<>();
        for (int[] rBucket : bucketsFromR) {
            int costR = rBucket[1];
            int costS = 0;
            int prev = 0;

            buffForR.position(13);
            buffForR.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
            buffForR.putInt(rBucket[0]);
            iFrameTupleAccessorForR.reset(buffForR);

            for (int[] sBucket : bucketsFromS) {

                buffForS.position(13);
                buffForS.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
                buffForS.putInt(sBucket[0]);
                iFrameTupleAccessorForS.reset(buffForS);

                if (comparator.compare(iFrameTupleAccessorForR, 0, iFrameTupleAccessorForS, 0) < 1) {
                    costS += sBucket[1];
                    /*if(prev + 1 != j) {
                        costS += SEEK_PENALTY;
                    }*/
                }
            }

            if (costS == 0)
                continue;

            int[] newBucketR = new int[7];
            newBucketR[0] = rBucket[0];
            newBucketR[1] = rBucket[1];
            newBucketR[2] = rBucket[2];
            newBucketR[3] = rBucket[3];
            newBucketR[4] = rBucket[4];
            newBucketR[5] = rBucket[5];
            newBucketR[6] = (int) Math.ceil(((double) costS / costR) * 10000);
            tempList.add(newBucketR);

        }
        bucketsFromR.clear();
        bucketsFromR.addAll(tempList);
        bucketsFromR.sort(Comparator.comparingDouble(o -> -o[6]));

        tempList.clear();

        for (int[] sBucket : bucketsFromS) {
            int costS = sBucket[1];
            int costR = 0;
            int prev = 0;

            buffForS.position(13);
            buffForS.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
            buffForS.putInt(sBucket[0]);
            iFrameTupleAccessorForS.reset(buffForS);

            for (int[] rBucket : bucketsFromR) {

                buffForR.position(13);
                buffForR.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
                buffForR.putInt(rBucket[0]);
                iFrameTupleAccessorForR.reset(buffForR);

                if (comparator.compare(iFrameTupleAccessorForR, 0, iFrameTupleAccessorForS, 0) < 1) {
                    costR += rBucket[1];
                    /*if(prev + 1 != j) {
                        costR += SEEK_PENALTY;
                    }*/
                }
            }

            if (costR == 0)
                continue;

            int[] newBucket = new int[7];
            newBucket[0] = sBucket[0];
            newBucket[1] = sBucket[1];
            newBucket[2] = sBucket[2];
            newBucket[3] = sBucket[3];
            newBucket[4] = sBucket[4];
            newBucket[5] = sBucket[5];
            newBucket[6] = (int) Math.ceil(((double) costR / costS) * 10000);
            tempList.add(newBucket);

        }
        bucketsFromS.clear();
        bucketsFromS.addAll(tempList);
        bucketsFromS.sort(Comparator.comparingDouble(o -> -o[6]));
        //System.out.println("test");
    }

    public void setBucketTable(SerializableBucketIdList bucketTable) throws HyracksDataException {
        this.bucketTable = bucketTable;
        this.numberOfBuckets = bucketTable.getNumEntries();
        this.bucketsFromR = new ArrayList<>();
        this.bucketsFromS = new ArrayList<>();

        tempBucketsFromR = new ArrayList<>();
        for (int i = 0; i < this.numberOfBuckets; i++) {
            int[] bucket = bucketTable.getEntry(i);
            if (bucket[0] == -1 || bucket[1] > -1 || bucket[2] == -1) {
                continue;
            }
            tempBucketsFromR.add(bucket);
        }
        tempBucketsFromR.sort(Comparator.comparingDouble(o -> -o[1]));

        tempBucketsFromS = new ArrayList<>();
        for (int i = 0; i < this.numberOfBuckets; i++) {
            int[] bucket = bucketTable.getEntry(i);
            if (bucket[0] == -1 || bucket[3] > -1 || bucket[4] == -1) {
                continue;
            }
            tempBucketsFromS.add(bucket);
        }
        tempBucketsFromS.sort(Comparator.comparingDouble(o -> -o[3]));

        ArrayList<int[]> tempTwoR = new ArrayList<>();
        ArrayList<int[]> tempTwoS = new ArrayList<>();

        for (int i = 0; i < tempBucketsFromR.size(); i++) {
            int[] bucket = tempBucketsFromR.get(i);
            int bucketSize;
            int startFrame = -((bucket[1] + 1));
            int startOffset = bucket[2];
            int startOffsetInFile = -((bucket[1] + 1) * this.frameSize) + bucket[2];
            int[] nextBucket = new int[5];
            int endFrame;
            int endOffset;
            if (i + 1 < tempBucketsFromR.size()) {
                int nextOnDisk;
                for (nextOnDisk = i + 1; nextOnDisk < tempBucketsFromR.size(); nextOnDisk++) {
                    nextBucket = tempBucketsFromR.get(nextOnDisk);
                    if (nextBucket[1] < 0)
                        break;
                }
                endFrame = -(nextBucket[1] + 1);
                endOffset = nextBucket[2];
                bucketSize = ((endFrame * this.frameSize) + endOffset) - startOffsetInFile;

            } else {
                endFrame = -1;
                endOffset = -1;
                bucketSize = (int) ((buildFileSize + 5) - startOffsetInFile);
            }

            //This part is implemented by assuming every bucket will start from a new frame
            if (bucketSize > memoryForJoinInBytes) {
                int tempBucketSize = bucketSize;
                while (tempBucketSize > 0) {
                    int currentBucketSize = Math.min(memoryForJoinInBytes, tempBucketSize);
                    int[] newBucket = new int[6];
                    newBucket[0] = bucket[0];
                    newBucket[1] = currentBucketSize;
                    newBucket[2] = startFrame;
                    newBucket[3] = 5;
                    newBucket[4] = startFrame + (currentBucketSize / frameSize);
                    newBucket[5] = 5;
                    tempTwoR.add(newBucket);
                    startFrame += (currentBucketSize / frameSize);
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
                tempTwoR.add(newBucket);
            }
            //            int[] newBucket = new int[5];
            //            newBucket[0] = bucket[0];
            //            newBucket[1] = bucketSize;
            //            newBucket[2] = endFrame;
            //            newBucket[3] = endOffset;
            //            newBucket[4] = i;
            //            tempTwoR.add(newBucket);
        }

        for (int i = 0; i < tempBucketsFromS.size(); i++) {
            int[] bucket = tempBucketsFromS.get(i);
            int bucketSize;
            int startFrame = -((bucket[3] + 1));
            int startOffset = bucket[4];
            int startOffsetInFile = -((bucket[3] + 1) * this.frameSize) + bucket[4];
            int[] nextBucket = new int[5];
            int endFrame;
            int endOffset;
            if (i + 1 < tempBucketsFromS.size()) {
                int nextOnDisk;
                for (nextOnDisk = i + 1; nextOnDisk < tempBucketsFromS.size(); nextOnDisk++) {
                    nextBucket = tempBucketsFromS.get(nextOnDisk);
                    if (nextBucket[3] < 0)
                        break;
                }
                endFrame = -(nextBucket[3] + 1);
                endOffset = nextBucket[4];
                bucketSize = ((endFrame * this.frameSize) + endOffset) - startOffsetInFile;

            } else {
                endFrame = -1;
                endOffset = -1;
                bucketSize = (int) ((probeFileSize + 5) - startOffsetInFile);
            }
            //This part is implemented by assuming every bucket will start from a new frame
            if (bucketSize > memoryForJoinInBytes) {
                int tempBucketSize = bucketSize;
                while (tempBucketSize > 0) {
                    int currentBucketSize = Math.min(memoryForJoinInBytes, tempBucketSize);
                    int[] newBucket = new int[6];
                    newBucket[0] = bucket[0];
                    newBucket[1] = currentBucketSize;
                    newBucket[2] = startFrame;
                    newBucket[3] = 5;
                    newBucket[4] = startFrame + (currentBucketSize / frameSize);
                    newBucket[5] = 5;
                    tempTwoS.add(newBucket);
                    startFrame += (currentBucketSize / frameSize);
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
                tempTwoS.add(newBucket);
            }
            //            int[] newBucket = new int[5];
            //            newBucket[0] = bucket[0];
            //            newBucket[1] = bucketSize;
            //            newBucket[2] = endFrame;
            //            newBucket[3] = endOffset;
            //            newBucket[4] = i;
            //            tempTwoS.add(newBucket);
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

        for (int i = 0; i < tempTwoR.size(); i++) {
            int costR = tempTwoR.get(i)[1];
            int costS = 0;
            int prev = 0;

            buffForR.position(13);
            buffForR.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
            buffForR.putInt(tempTwoR.get(i)[0]);
            iFrameTupleAccessorForR.reset(buffForR);

            for (int j = 0; j < tempTwoS.size(); j++) {

                buffForS.position(13);
                buffForS.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
                buffForS.putInt(tempTwoS.get(j)[0]);
                iFrameTupleAccessorForS.reset(buffForS);

                if (comparator.compare(iFrameTupleAccessorForR, 0, iFrameTupleAccessorForS, 0) < 1) {
                    costS += tempTwoS.get(j)[1];
                    /*if(prev + 1 != j) {
                        costS += SEEK_PENALTY;
                    }*/
                }
            }

            if (costS == 0)
                continue;

            int[] newBucket = new int[7];
            newBucket[0] = tempTwoR.get(i)[0];
            newBucket[1] = tempTwoR.get(i)[1];
            newBucket[2] = tempTwoR.get(i)[2];
            newBucket[3] = tempTwoR.get(i)[3];
            newBucket[4] = tempTwoR.get(i)[4];
            newBucket[5] = tempTwoR.get(i)[5];
            newBucket[6] = (int) Math.ceil(((double) costS / costR) * 10000);
            bucketsFromR.add(newBucket);

        }
        bucketsFromR.sort(Comparator.comparingDouble(o -> -o[6]));

        for (int i = 0; i < tempTwoS.size(); i++) {
            int costS = tempTwoS.get(i)[1];
            int costR = 0;
            int prev = 0;

            buffForS.position(13);
            buffForS.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
            buffForS.putInt(tempTwoS.get(i)[0]);
            iFrameTupleAccessorForS.reset(buffForS);

            for (int j = 0; j < tempTwoR.size(); j++) {

                buffForR.position(13);
                buffForR.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
                buffForR.putInt(tempTwoR.get(j)[0]);
                iFrameTupleAccessorForR.reset(buffForR);

                if (comparator.compare(iFrameTupleAccessorForR, 0, iFrameTupleAccessorForS, 0) < 1) {
                    costR += tempTwoR.get(j)[1];
                    /*if(prev + 1 != j) {
                        costR += SEEK_PENALTY;
                    }*/
                }
            }

            if (costR == 0)
                continue;

            int[] newBucket = new int[7];
            newBucket[0] = tempTwoS.get(i)[0];
            newBucket[1] = tempTwoS.get(i)[1];
            newBucket[2] = tempTwoS.get(i)[2];
            newBucket[3] = tempTwoS.get(i)[3];
            newBucket[4] = tempTwoS.get(i)[4];
            newBucket[5] = tempTwoS.get(i)[5];
            newBucket[6] = (int) Math.ceil(((double) costR / costS) * 10000);
            bucketsFromS.add(newBucket);

        }
        bucketsFromS.sort(Comparator.comparingDouble(o -> -o[6]));
        //System.out.println("test");
    }

    @Override
    public void setComparator(ITuplePairComparator comparator) {
        this.comparator = comparator;
    }
}
