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
    boolean hasNextBuildingBucketSequence;
    int numberOfBuckets;
    public FirstFit(SerializableBucketIdList bucketTable, int memoryForJoinInBytes, RunFileStream buildRunFileStream, RunFileStream probeRunFileStream, RecordDescriptor buildRecordDescriptor, RecordDescriptor probeRecordDescriptor) throws HyracksDataException {
        this.bucketTable = bucketTable;
        this.memoryForJoinInBytes = memoryForJoinInBytes;
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
    public ArrayList<IBucket> nextBuildingBucketSequence() {
        ArrayList<IBucket> returnBuckets = new ArrayList<>();
        int totalSizeOfBuckets = 0;
        int previousOffset = 0;
        boolean first = true;

        for(int i = 0; i < this.numberOfBuckets; i++) {
            int[] bucket = bucketTable.getEntry(i);
            if(bucket[1] > 0) continue;
            totalSizeOfBuckets += (first)?0:(-bucket[2] * bucket[3] - previousOffset);
            if(totalSizeOfBuckets > memoryForJoinInBytes) break;
            returnBuckets.add(new Bucket(bucket[0]));

            if(!first) previousOffset = -bucket[2] * bucket[3];
            first = false;
        }
        hasNextBuildingBucketSequence = false;
        return returnBuckets;
    }

    @Override
    public ArrayList<IBucket> nextProbingBucketSequence() {
        return new ArrayList<>();
    }
}
