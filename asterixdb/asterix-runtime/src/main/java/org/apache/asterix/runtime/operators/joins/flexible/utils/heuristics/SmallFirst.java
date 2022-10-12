package org.apache.asterix.runtime.operators.joins.flexible.utils.heuristics;

import com.google.common.collect.Lists;
import org.apache.asterix.runtime.operators.joins.flexible.utils.Bucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IBucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IHeuristicForThetaJoin;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.structures.SerializableBucketIdList;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

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

    ArrayList<Long[]> bucketsFromR;



    public SmallFirst(int memoryForJoin, int frameSize, long buildFileSize, long probeFileSize) throws HyracksDataException {
        this.memoryForJoinInBytes = memoryForJoin * frameSize;
        this.memoryForJoinInFrames = memoryForJoin;
        this.frameSize = frameSize;
        this.buildFileSize = buildFileSize;
        this.probeFileSize = probeFileSize;
        this.hasNextBuildingBucketSequence = true;


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
        ArrayList<Long[]> removeList = new ArrayList<>();
        for(Long[] bucket: bucketsFromR) {

            long entryInTable = bucket[2];
            int[] bucketInfoFromTable = bucketTable.getEntry((int) entryInTable);

            long bucketSize;
            long startOffsetInFile = -(((long) (bucketInfoFromTable[1] + 1) * this.frameSize) + bucketInfoFromTable[2]);
            int endFrame;
            int endOffset;
            if(entryInTable+1 < this.numberOfBuckets) {
                bucketSize = -((long) (bucketTable.getEntry((int) (entryInTable + 1))[1] + 1) * this.frameSize) + bucketTable.getEntry((int) (entryInTable+1))[2] - startOffsetInFile;
                endFrame = -(bucketTable.getEntry((int) (entryInTable + 1))[1] + 1);
                endOffset = bucketTable.getEntry((int) (entryInTable+1))[2];
            } else {
                bucketSize = buildFileSize - startOffsetInFile;
                endFrame = -1;
                endOffset = -1;
            }

            if(Math.ceil(((double)totalSizeForBuckets + bucketSize)*CONSTANT/frameSize) < memoryForJoinInFrames) totalSizeForBuckets += bucketSize;
            else break;

            removeList.add(bucket);
            Bucket returnBucket = new Bucket(bucketInfoFromTable[0],0, bucketInfoFromTable[2], endOffset, -(bucketInfoFromTable[1]+1), endFrame);
            returnBuckets.add(returnBucket);
        }
        bucketsFromR.removeAll(removeList);
        return returnBuckets;
    }

    @Override
    public ArrayList<IBucket> nextProbingBucketSequence() {
        ArrayList<IBucket> returnBuckets = new ArrayList<>();
        int totalSizeOfBuckets = 0;

        for(int i= 0; i < this.numberOfBuckets; i++) {
            int[] bucket = bucketTable.getEntry(i);

            if(bucket[0] == -1 || bucket[4] == -1) {
                continue;
            }
            if(bucket[3] >= 0) continue;
            long bucketSize;
            long startOffsetInFile = -((long) (bucket[3] + 1) * this.frameSize) + bucket[4];
            int endFrame;
            int endOffset;
            if(i+1 < this.numberOfBuckets) {
                bucketSize = -((long) (bucketTable.getEntry(i + 1)[3] + 1) * this.frameSize) + bucketTable.getEntry(i+1)[4] - startOffsetInFile;
                endOffset = bucketTable.getEntry(i+1)[4];
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
            Bucket returnBucket = new Bucket(bucket[0],1, bucket[4], endOffset, -(bucket[3]+1), endFrame);
            returnBuckets.add(returnBucket);
        }

        return returnBuckets;
    }

    public void setBucketTable(SerializableBucketIdList bucketTable) {
        this.bucketTable = bucketTable;
        this.numberOfBuckets = bucketTable.getNumEntries();
        this.bucketsFromR = new ArrayList<>();
        for(int i= 0; i < this.numberOfBuckets; i++) {
            int[] bucket = bucketTable.getEntry(i);
            if(bucket[0] == -1 || bucket[1] > -1 || bucket[2] == -1) {
                continue;
            }
            long bucketSize;
            long startOffsetInFile = -((long) (bucket[1] + 1) * this.frameSize) + bucket[2];
            if(i+1 < this.numberOfBuckets) {
                bucketSize = -((long) (bucketTable.getEntry(i + 1)[1] + 1) * this.frameSize) + bucketTable.getEntry(i+1)[2] - startOffsetInFile;
            } else {
                bucketSize = (buildFileSize+5) - startOffsetInFile;
            }
            Long[] newBucket = new Long[3];
            newBucket[0] = (long) bucket[0];
            newBucket[1] = bucketSize;
            newBucket[2] = (long) i;
            this.bucketsFromR.add(newBucket);
        }
        bucketsFromR.sort(Comparator.comparingDouble(o -> o[1]));
    }
}
