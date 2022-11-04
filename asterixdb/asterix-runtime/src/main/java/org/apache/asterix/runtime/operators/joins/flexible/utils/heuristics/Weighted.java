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
    int IOSeek = 2;
    int IOSequel = 1;
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

    private int[] buildKeys;
    private int[] probeKeys;

    private byte[] byteArrayForTempBucketTupleR;
    private byte[] byteArrayForTempBucketTupleS;

    private ByteBuffer buffForTempBucketTupleR;
    private ByteBuffer buffForTempBucketTupleS;

    private IFrameTupleAccessor iFrameTupleAccessorForTempBucketTupleR;
    private IFrameTupleAccessor iFrameTupleAccessorForTempBucketTupleS;

    boolean continueToCheckBuckets = false;

    public Weighted(int memoryForJoin, int frameSize, long buildFileSize, long probeFileSize, RecordDescriptor buildRd,
            RecordDescriptor probeRd, int[] buildKeys, int[] probeKeys, boolean checkForRoleReversal,
            boolean continueToCheckBuckets) throws HyracksDataException {
        this.memoryForJoinInBytes = memoryForJoin * frameSize;
        this.memoryForJoinInFrames = memoryForJoin;
        this.frameSize = frameSize;
        this.buildFileSize = buildFileSize;
        this.probeFileSize = probeFileSize;
        this.hasNextBuildingBucketSequence = true;
        this.buildRd = buildRd;
        this.probeRd = probeRd;
        this.checkForRoleReversal = checkForRoleReversal;

        this.buildKeys = buildKeys;
        this.probeKeys = probeKeys;

        this.byteArrayForTempBucketTupleR = new byte[buildRd.getFieldCount() * 4 + 5 + 5];
        this.byteArrayForTempBucketTupleS = new byte[probeRd.getFieldCount() * 4 + 5 + 5];

        this.buffForTempBucketTupleR = ByteBuffer.wrap(this.byteArrayForTempBucketTupleR);
        this.buffForTempBucketTupleS = ByteBuffer.wrap(this.byteArrayForTempBucketTupleS);

        this.iFrameTupleAccessorForTempBucketTupleR = new FrameTupleAccessor(buildRd);
        this.iFrameTupleAccessorForTempBucketTupleS = new FrameTupleAccessor(probeRd);

        this.continueToCheckBuckets = continueToCheckBuckets;
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
        String rbr = "";
        String rbs = "";
        for (int i = 0; i < bucketsFromR.size(); i++) {
            int[] bucket = bucketsFromR.get(i);

            int bucketSize = bucket[1];
            int startFrame = bucket[2];
            int startOffset = bucket[3];

            int endFrame = bucket[4];
            int endOffset = bucket[5];

            if (this.continueToCheckBuckets) {
                if (Math.ceil(
                        ((double) totalSizeForBuckets + bucketSize) * CONSTANT / frameSize) <= memoryForJoinInFrames) {
                    totalSizeForBuckets += bucketSize;
                    costR += bucket[6];
                    removeListR.add(bucket);
                    rbr += bucket[0] + ",";
                    Bucket returnBucket = new Bucket(bucket[0], 0, startOffset, endOffset, startFrame, endFrame);
                    returnBucketsR.add(returnBucket);
                }
            } else {
                if (!(Math.ceil(
                        ((double) totalSizeForBuckets + bucketSize) * CONSTANT / frameSize) <= memoryForJoinInFrames)) {
                    break;
                }
                totalSizeForBuckets += bucketSize;
                costR += bucket[6];
                removeListR.add(bucket);
                rbr += bucket[0] + ",";
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
            if (this.continueToCheckBuckets) {
            if (Math.ceil(
                    ((double) totalSizeForBuckets + bucketSize) * CONSTANT / frameSize) <= memoryForJoinInFrames) {
                totalSizeForBuckets += bucketSize;
                costS += bucket[6];
                rbs += bucket[0] + ",";
                removeListS.add(bucket);
                Bucket returnBucket = new Bucket(bucket[0], 1, startOffset, endOffset, startFrame, endFrame);
                returnBucketsS.add(returnBucket);
            }} else {
                if (Math.ceil(
                        ((double) totalSizeForBuckets + bucketSize) * CONSTANT / frameSize) > memoryForJoinInFrames)
                    break;
                    totalSizeForBuckets += bucketSize;
                    costS += bucket[6];
                    rbs += bucket[0] + ",";
                    removeListS.add(bucket);
                    Bucket returnBucket = new Bucket(bucket[0], 1, startOffset, endOffset, startFrame, endFrame);
                    returnBucketsS.add(returnBucket);

            }

        }
        if (checkForRoleReversal && costS < costR) {
            bucketsFromS.removeAll(removeListS);
            probingBucketSequence = bucketsFromR;
            roleReversal = true;
            returnBucketsS.sort(Comparator.comparingDouble(IBucket::getStartFrame));
            returnedBuildingBucketSequence = returnBucketsS;
            System.out.println("Removed Buckets From S: " + rbs);
            return returnBucketsS;
        } else {
            roleReversal = false;
            bucketsFromR.removeAll(removeListR);
            probingBucketSequence = bucketsFromS;
            returnBucketsR.sort(Comparator.comparingDouble(IBucket::getStartFrame));
            returnedBuildingBucketSequence = returnBucketsR;
            System.out.println("Removed Buckets From R: " + rbr);

            return returnBucketsR;
        }
        //        bucketsFromS.removeAll(removeListS);
        //        return returnBucketsS;
    }

    public ArrayList<IBucket> nextProbingBucketSequence() throws HyracksDataException {
        ArrayList<IBucket> returnBuckets = new ArrayList<>();
        String rb = "";
        for (int[] bucket : probingBucketSequence) {

            int startFrame = bucket[2];
            int starOffset = bucket[3];;

            int endFrame = bucket[4];
            int endOffset = bucket[5];

            if (!roleReversal) {
                setTupleAccessorForTempBucketTupleS(bucket[0]);

                for (IBucket rBucket : returnedBuildingBucketSequence) {

                    setTupleAccessorForTempBucketTupleR(rBucket.getBucketId());
                    if (comparator.compare(iFrameTupleAccessorForTempBucketTupleR, 0,
                            iFrameTupleAccessorForTempBucketTupleS, 0) < 1) {
                        Bucket returnBucket = new Bucket(bucket[0], 1, starOffset, endOffset, startFrame, endFrame);
                        returnBuckets.add(returnBucket);
                        rb += bucket[0] + ",";
                    }
                }
            } else {
                setTupleAccessorForTempBucketTupleR(bucket[0]);

                for (IBucket sBucket : returnedBuildingBucketSequence) {

                    setTupleAccessorForTempBucketTupleS(sBucket.getBucketId());

                    if (comparator.compare(iFrameTupleAccessorForTempBucketTupleR, 0,
                            iFrameTupleAccessorForTempBucketTupleS, 0) < 1) {
                        Bucket returnBucket = new Bucket(bucket[0], 0, starOffset, endOffset, startFrame, endFrame);
                        returnBuckets.add(returnBucket);
                        rb += bucket[0] + ",";
                    }
                }
            }

        }
        returnBuckets.sort(Comparator.comparingDouble(IBucket::getStartFrame));
        reComputeCosts();
        System.out.println("Processed Buckets From " + (roleReversal?"R":"S") + ": " + rb);
        return returnBuckets;
    }

    private void setTupleAccessorForTempBucketTupleR(int bucketId) {
        this.buffForTempBucketTupleR.position(this.buildKeys[0] * 4 + 5 + 4);
        this.buffForTempBucketTupleR.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
        this.buffForTempBucketTupleR.putInt(bucketId);
        this.iFrameTupleAccessorForTempBucketTupleR.reset(this.buffForTempBucketTupleR);
    }

    private void setTupleAccessorForTempBucketTupleS(int bucketId) {
        this.buffForTempBucketTupleS.position(this.probeKeys[0] * 4 + 5 + 4);
        this.buffForTempBucketTupleS.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
        this.buffForTempBucketTupleS.putInt(bucketId);
        this.iFrameTupleAccessorForTempBucketTupleS.reset(this.buffForTempBucketTupleS);
    }

    public void reComputeCosts() throws HyracksDataException {

        ArrayList<int[]> tempList = new ArrayList<>();
        for (int[] rBucket : bucketsFromR) {
            int costR = rBucket[1] * IOSequel;
            int costS = 0;
            int prev = 0;

            setTupleAccessorForTempBucketTupleR(rBucket[0]);

            for (int sBucketIdx = 0; sBucketIdx < bucketsFromS.size(); sBucketIdx++) {
                int[] sBucket = bucketsFromS.get(sBucketIdx);

                setTupleAccessorForTempBucketTupleS(sBucket[0]);

                if (comparator.compare(iFrameTupleAccessorForTempBucketTupleR, 0,
                        iFrameTupleAccessorForTempBucketTupleS, 0) < 1) {
                    costS += sBucket[1] * IOSequel;
                    if (prev + 1 != sBucketIdx) {
                        costS += IOSeek;
                    }
                    prev = sBucketIdx;
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
            newBucketR[6] = (int) Math.ceil(((double) costS + costR));
            tempList.add(newBucketR);

        }
        bucketsFromR.clear();
        bucketsFromR.addAll(tempList);
        bucketsFromR.sort(Comparator.comparingDouble(o -> o[6]));

        tempList.clear();

        for (int[] sBucket : bucketsFromS) {
            int costS = sBucket[1] * IOSequel;
            int costR = 0;
            int prev = 0;

            setTupleAccessorForTempBucketTupleS(sBucket[0]);

            for (int rBucketIdx = 0; rBucketIdx < bucketsFromR.size(); rBucketIdx++) {
                int[] rBucket = bucketsFromR.get(rBucketIdx);

                setTupleAccessorForTempBucketTupleR(rBucket[0]);

                if (comparator.compare(iFrameTupleAccessorForTempBucketTupleR, 0,
                        iFrameTupleAccessorForTempBucketTupleS, 0) < 1) {
                    costR += rBucket[1] * IOSequel;
                    if (prev + 1 != rBucketIdx) {
                        costR += IOSeek;
                    }
                    prev = rBucketIdx;
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
            newBucket[6] = (int) Math.ceil(((double) costR + costS));
            tempList.add(newBucket);

        }
        bucketsFromS.clear();
        bucketsFromS.addAll(tempList);
        bucketsFromS.sort(Comparator.comparingDouble(o -> o[6]));
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

        for (int i = 0; i < tempTwoR.size(); i++) {
            int costR = tempTwoR.get(i)[1] * IOSequel;
            int costS = 0;
            int prev = 0;

            setTupleAccessorForTempBucketTupleR(tempTwoR.get(i)[0]);

            for (int j = 0; j < tempTwoS.size(); j++) {

                setTupleAccessorForTempBucketTupleS(tempTwoS.get(j)[0]);

                if (comparator.compare(iFrameTupleAccessorForTempBucketTupleR, 0,
                        iFrameTupleAccessorForTempBucketTupleS, 0) < 1) {
                    costS += tempTwoS.get(j)[1] * IOSequel;
                    if (prev + 1 != j) {
                        costS += IOSeek;
                    }
                    prev = j;
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
            newBucket[6] = (int) Math.ceil(((double) costS + costR));
            bucketsFromR.add(newBucket);

        }
        bucketsFromR.sort(Comparator.comparingDouble(o -> o[6]));

        for (int i = 0; i < tempTwoS.size(); i++) {
            int costS = tempTwoS.get(i)[1] * IOSequel;
            int costR = 0;
            int prev = 0;

            setTupleAccessorForTempBucketTupleS(tempTwoS.get(i)[0]);

            for (int j = 0; j < tempTwoR.size(); j++) {

                setTupleAccessorForTempBucketTupleR(tempTwoR.get(j)[0]);

                if (comparator.compare(iFrameTupleAccessorForTempBucketTupleR, 0,
                        iFrameTupleAccessorForTempBucketTupleS, 0) < 1) {
                    costR += tempTwoR.get(j)[1] * IOSequel;
                    if (prev + 1 != j) {
                        costR += IOSeek;
                    }
                    prev = j;
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
            newBucket[6] = (int) Math.ceil(((double) costR + costS));
            bucketsFromS.add(newBucket);

        }
        bucketsFromS.sort(Comparator.comparingDouble(o -> o[6]));
        //System.out.println("test");
    }

    @Override
    public void setComparator(ITuplePairComparator comparator) {
        this.comparator = comparator;
    }
}
