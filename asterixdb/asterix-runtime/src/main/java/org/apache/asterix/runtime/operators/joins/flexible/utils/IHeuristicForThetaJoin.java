package org.apache.asterix.runtime.operators.joins.flexible.utils;

import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFileStream;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.structures.SerializableBucketIdList;

import java.util.ArrayList;

public interface IHeuristicForThetaJoin {
//    default IHeuristicForThetaJoin(SerializableBucketIdList bucketTable, RunFileStream buildRunFileStream, RunFileStream probeRunFileStream) {
//
//    }

    boolean hasNextBuildingBucketSequence();
    ArrayList<IBucket> nextBuildingBucketSequence() throws HyracksDataException;
    ArrayList<IBucket> nextProbingBucketSequence();
    void setBucketTable(SerializableBucketIdList bucketTable);

}
