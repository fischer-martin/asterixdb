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
import java.util.Comparator;

import org.apache.asterix.runtime.operators.joins.flexible.utils.Bucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IBucket;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.structures.SerializableBucketIdList;

public class Weighted extends AbstractHeuristic {

    ArrayList<int[]> probingBucketSequence;
    double totalCost = 0;

    public Weighted(int memoryForJoin, int frameSize, long buildFileSize, long probeFileSize, RecordDescriptor buildRd,
            RecordDescriptor probeRd, int[] buildKeys, int[] probeKeys, boolean checkForRoleReversal,
            boolean continueToCheckBuckets) throws HyracksDataException {
        super(memoryForJoin, frameSize, buildFileSize, probeFileSize, buildRd, probeRd, buildKeys, probeKeys, checkForRoleReversal, continueToCheckBuckets);
    }

    @Override
    public boolean hasNextBuildingBucketSequence() {
        return !bucketsFromR.isEmpty() && !bucketsFromS.isEmpty();
    }
    @Override
    public ArrayList<IBucket> nextBuildingBucketSequence() throws HyracksDataException {
        buildingBucketSequence.clear();

        ArrayList<IBucket> returnBucketsR = new ArrayList<>();
        ArrayList<IBucket> returnBucketsS = new ArrayList<>();

        long totalSizeForBuckets = 0;

        ArrayList<int[]> removeListR = new ArrayList<>();
        ArrayList<int[]> removeListS = new ArrayList<>();

        int costR = 0;
        int costS = 0;

        for (int[] bucket : bucketsFromR) {
            int bucketSize = bucket[1];

            int endFrame = bucket[4];
            int endOffset = bucket[5];
            if (this.continueToCheckBuckets) {
                if (Math.ceil(
                        ((double) totalSizeForBuckets + bucketSize) / frameSize) <= memoryForJoinInFrames) {
                    totalSizeForBuckets += bucketSize;
                    removeListR.add(bucket);
                    Bucket returnBucket;
                    returnBucket =
                            new Bucket(bucket[0], 0, bucket[3], endOffset, bucket[2], endFrame);
                    returnBucketsR.add(returnBucket);
                    costR += bucket[6];
                }
            } else {
                if (Math.ceil(
                        ((double) totalSizeForBuckets + bucketSize) / frameSize) <= memoryForJoinInFrames) {
                    totalSizeForBuckets += bucketSize;

                } else
                    break;
                removeListR.add(bucket);
                Bucket returnBucket;
                returnBucket = new Bucket(bucket[0], 0, bucket[3], endOffset, bucket[2], endFrame);
                returnBucketsR.add(returnBucket);
                costR += bucket[6];
            }


        }
        totalSizeForBuckets = 0;
        for (int[] bucket : bucketsFromS) {
            int bucketSize = bucket[1];

            int endFrame = bucket[4];
            int endOffset = bucket[5];
            if (this.continueToCheckBuckets) {
                if (Math.ceil(
                        ((double) totalSizeForBuckets + bucketSize) / frameSize) <= memoryForJoinInFrames) {
                    totalSizeForBuckets += bucketSize;
                    removeListS.add(bucket);
                    Bucket returnBucket;
                    returnBucket =
                            new Bucket(bucket[0], 1, bucket[3], endOffset, bucket[2], endFrame);
                    returnBucketsS.add(returnBucket);
                    costS += bucket[6];
                }
            } else {
                if (Math.ceil(
                        ((double) totalSizeForBuckets + bucketSize) / frameSize) <= memoryForJoinInFrames) {
                    totalSizeForBuckets += bucketSize;

                } else
                    break;
                removeListS.add(bucket);
                Bucket returnBucket;
                returnBucket = new Bucket(bucket[0], 1, bucket[3], endOffset, bucket[2], endFrame);
                returnBucketsS.add(returnBucket);
                costS += bucket[6];
            }


        }

        if(checkForRoleReversal && costS < costR) {
            roleReversal = true;
            bucketsFromS.removeAll(removeListS);
            buildingBucketSequence = returnBucketsS;
            probingBucketSequence = bucketsFromR;
            System.out.println("Cost of the building Buckets " + costS);
            totalCost += costS;
        } else {
            roleReversal = false;
            bucketsFromR.removeAll(removeListR);
            buildingBucketSequence = returnBucketsR;
            probingBucketSequence = bucketsFromS;
            totalCost += costR;
            //System.out.println("Cost of the building Buckets " + costR);

        }
        buildingBucketSequence.sort(Comparator.comparingDouble(IBucket::getStartFrame));
        return buildingBucketSequence;
    }
    /*@Override
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
                        ((double) totalSizeForBuckets + bucketSize) / frameSize) <= memoryForJoinInFrames) {
                    totalSizeForBuckets += bucketSize;
                    costR += bucket[6];
                    removeListR.add(bucket);
                    rbr += bucket[0] + ",";
                    Bucket returnBucket = new Bucket(bucket[0], 0, startOffset, endOffset, startFrame, endFrame);
                    returnBucketsR.add(returnBucket);
                }
            } else {
                if (!(Math.ceil(
                        ((double) totalSizeForBuckets + bucketSize) / frameSize) <= memoryForJoinInFrames)) {
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
                    ((double) totalSizeForBuckets + bucketSize) / frameSize) <= memoryForJoinInFrames) {
                totalSizeForBuckets += bucketSize;
                costS += bucket[6];
                rbs += bucket[0] + ",";
                removeListS.add(bucket);
                Bucket returnBucket = new Bucket(bucket[0], 1, startOffset, endOffset, startFrame, endFrame);
                returnBucketsS.add(returnBucket);
            }} else {
                if (Math.ceil(
                        ((double) totalSizeForBuckets + bucketSize) / frameSize) > memoryForJoinInFrames)
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
    }*/

    public ArrayList<IBucket> nextProbingBucketSequence() throws HyracksDataException {
        ArrayList<IBucket> returnBuckets = new ArrayList<>();
        //String rb = "";
        for (int[] bucket : probingBucketSequence) {

            int startFrame = bucket[2];
            int starOffset = bucket[3];;

            int endFrame = bucket[4];
            int endOffset = bucket[5];

            if (!roleReversal) {
                setTupleAccessorForTempBucketTupleS(bucket[0]);

                for (IBucket rBucket : buildingBucketSequence) {

                    setTupleAccessorForTempBucketTupleR(rBucket.getBucketId());
                    if (compare()) {
                        Bucket returnBucket = new Bucket(bucket[0], 1, starOffset, endOffset, startFrame, endFrame);
                        returnBuckets.add(returnBucket);
                        break;
                        //rb += bucket[0] + ",";
                    }
                }
            } else {
                setTupleAccessorForTempBucketTupleR(bucket[0]);

                for (IBucket sBucket : buildingBucketSequence) {

                    setTupleAccessorForTempBucketTupleS(sBucket.getBucketId());

                    if (compare()) {
                        Bucket returnBucket = new Bucket(bucket[0], 0, starOffset, endOffset, startFrame, endFrame);
                        returnBuckets.add(returnBucket);
                        break;
                        //rb += bucket[0] + ",";
                    }
                }
            }

        }
        returnBuckets.sort(Comparator.comparingDouble(IBucket::getStartFrame));
        computeCosts();
        //System.out.println("Processed Buckets From " + (roleReversal?"R":"S") + ": " + rb);
        return returnBuckets;
    }

    public void computeCosts() throws HyracksDataException {

        ArrayList<int[]> tempTwoR = new ArrayList<>();
        ArrayList<int[]> tempTwoS = new ArrayList<>();

        for (int i = 0; i < bucketsFromR.size(); i++) {
            double costR = (bucketsFromR.get(i)[1]/frameSize) * IORnd;
            double costS = 0;
            int prev = 0;

            setTupleAccessorForTempBucketTupleR(bucketsFromR.get(i)[0]);

            for (int j = 0; j < bucketsFromS.size(); j++) {

                setTupleAccessorForTempBucketTupleS(bucketsFromS.get(j)[0]);

                if (compare()) {
                    if (prev + 1 != j) {
                        costS += (bucketsFromS.get(j)[1] / frameSize) * IORnd;;
                    } else costS += (bucketsFromS.get(j)[1] / frameSize) * IOSeq;
                    prev = j;
                }
            }

            if (costS == 0)
                continue;

            int[] newBucket = new int[7];
            newBucket[0] = bucketsFromR.get(i)[0];
            newBucket[1] = bucketsFromR.get(i)[1];
            newBucket[2] = bucketsFromR.get(i)[2];
            newBucket[3] = bucketsFromR.get(i)[3];
            newBucket[4] = bucketsFromR.get(i)[4];
            newBucket[5] = bucketsFromR.get(i)[5];
            newBucket[6] = (int) (costS + costR);
            tempTwoR.add(newBucket);

        }
        tempTwoR.sort(Comparator.comparingDouble(o -> o[6]));

        for (int i = 0; i < bucketsFromS.size(); i++) {
            double costS =  ((bucketsFromS.get(i)[1]/frameSize) * IORnd);
            double costR = 0;
            int prev = 0;

            setTupleAccessorForTempBucketTupleS(bucketsFromS.get(i)[0]);

            for (int j = 0; j < bucketsFromR.size(); j++) {

                setTupleAccessorForTempBucketTupleR(bucketsFromR.get(j)[0]);

                if (compare()) {

                    if (prev + 1 != j) {
                        costR += (bucketsFromR.get(j)[1] / frameSize) * IOSeq;
                    } else costR += (bucketsFromR.get(j)[1] / frameSize) * IORnd;
                    prev = j;
                }
            }

            if (costR == 0)
                continue;

            int[] newBucket = new int[7];
            newBucket[0] = bucketsFromS.get(i)[0];
            newBucket[1] = bucketsFromS.get(i)[1];
            newBucket[2] = bucketsFromS.get(i)[2];
            newBucket[3] = bucketsFromS.get(i)[3];
            newBucket[4] = bucketsFromS.get(i)[4];
            newBucket[5] = bucketsFromS.get(i)[5];
            newBucket[6] = (int) (costR + costS);
            tempTwoS.add(newBucket);

        }
        //tempTwoS.sort(Comparator.comparingDouble(o -> o[6]));

        bucketsFromR = tempTwoR;
        bucketsFromS = tempTwoS;
    }
    public void setBucketTable(SerializableBucketIdList bucketTable) throws HyracksDataException {
        retrieveBuckets(bucketTable);
        computeCosts();
    }
    /*public void setBucketTable(SerializableBucketIdList bucketTable) throws HyracksDataException {
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
    }*/

    public double getTotalCost() {
        return totalCost;
    }
}
