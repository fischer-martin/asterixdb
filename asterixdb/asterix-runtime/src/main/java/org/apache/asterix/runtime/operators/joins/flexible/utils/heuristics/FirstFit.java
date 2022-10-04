package org.apache.asterix.runtime.operators.joins.flexible.utils.heuristics;

import org.apache.asterix.runtime.operators.joins.flexible.utils.Bucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IBucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IHeuristicForThetaJoin;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.FrameTupleCursor;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFileStream;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.structures.SerializableBucketIdList;

import java.util.ArrayList;

public class FirstFit implements IHeuristicForThetaJoin {
    SerializableBucketIdList bucketTable;
    RunFileStream buildRunFileStream;
    RunFileStream probeRunFileStream;
    RecordDescriptor buildRecordDescriptor;
    RecordDescriptor probeRecordDescriptor;
    FrameTupleCursor frameTupleCursorForBuild;
    FrameTupleCursor frameTupleCursorForProbe;
    int memoryForJoinInBytes;
    int frameSize;
    boolean hasNextBuildingBucketSequence;
    int numberOfBuckets;
    int buildingBucketPosition = 0;
    public FirstFit(SerializableBucketIdList bucketTable, int memoryForJoin, int frameSize, RunFileStream buildRunFileStream, RunFileStream probeRunFileStream, RecordDescriptor buildRecordDescriptor, RecordDescriptor probeRecordDescriptor) throws HyracksDataException {
        this.bucketTable = bucketTable;
        this.memoryForJoinInBytes = memoryForJoin * frameSize;
        this.frameSize = frameSize;
        this.buildRunFileStream = buildRunFileStream;
        this.probeRunFileStream = probeRunFileStream;
        this.buildRecordDescriptor = buildRecordDescriptor;
        this.probeRecordDescriptor = probeRecordDescriptor;
        this.numberOfBuckets = bucketTable.getNumEntries();
        this.hasNextBuildingBucketSequence = true;

        this.frameTupleCursorForBuild = new FrameTupleCursor(buildRecordDescriptor);
        this.buildRunFileStream.startReadingRunFile(frameTupleCursorForBuild);

        this.frameTupleCursorForProbe = new FrameTupleCursor(probeRecordDescriptor);
        this.probeRunFileStream.startReadingRunFile(frameTupleCursorForProbe);

    }

    @Override
    public boolean hasNextBuildingBucketSequence() {
        return this.hasNextBuildingBucketSequence;
    }

    @Override
    public ArrayList<IBucket> nextBuildingBucketSequence() throws HyracksDataException {
        ArrayList<IBucket> returnBuckets = new ArrayList<>();
        int totalSizeOfBuckets = 0;
        int i;
        for(i = this.buildingBucketPosition; i < this.numberOfBuckets; i++) {
            int[] bucket = bucketTable.getEntry(i);

            if(bucket[0] == -1) {
                this.hasNextBuildingBucketSequence = false;
                return returnBuckets;
            }
            long bucketSize;
            long startOffset = -((long) (bucket[1] - 1) * this.frameSize) + bucket[2];
            if(i+1 < this.numberOfBuckets) {
                bucketSize = -((long) (bucketTable.getEntry(i + 1)[1] - 1) * this.frameSize) + bucketTable.getEntry(i+1)[2] - startOffset;
            } else {
                bucketSize = buildRunFileStream.getRunFileReaderSize() - startOffset;
            }
            if(bucket[1] >= 0) continue;
            totalSizeOfBuckets += bucketSize;
            if(totalSizeOfBuckets > memoryForJoinInBytes) break;
            Bucket returnBucket = new Bucket(bucket[0], startOffset, bucketSize, 0);
            returnBuckets.add(returnBucket);
        }
        this.buildingBucketPosition += i;
        if(this.buildingBucketPosition < this.numberOfBuckets) this.hasNextBuildingBucketSequence = true;
        else this.hasNextBuildingBucketSequence = false;
        return returnBuckets;
    }

    @Override
    public ArrayList<IBucket> nextProbingBucketSequence() {
        return new ArrayList<>();
    }
}
