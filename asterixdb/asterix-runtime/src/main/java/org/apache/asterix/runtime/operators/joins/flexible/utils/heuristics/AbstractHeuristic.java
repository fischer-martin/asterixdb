package org.apache.asterix.runtime.operators.joins.flexible.utils.heuristics;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IBucket;
import org.apache.asterix.runtime.operators.joins.flexible.utils.IHeuristicForThetaJoin;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.structures.SerializableBucketIdList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;

public abstract class AbstractHeuristic implements IHeuristicForThetaJoin {
    protected SerializableBucketIdList bucketTable;
    protected ITuplePairComparator comparator = null;

    long buildFileSize;
    long probeFileSize;
    long realBuildSize;
    int memoryForJoinInBytes;
    int memoryForJoinInFrames;
    int frameSize;
    boolean hasNextBuildingBucketSequence;
    int numberOfBuckets;
    int buildingBucketPosition = 0;

    ArrayList<int[]> bucketsFromR;
    ArrayList<int[]> bucketsFromS;
    ArrayList<int[]> tempBucketsFromR;
    ArrayList<int[]> tempBucketsFromS;

    RecordDescriptor buildRd;
    RecordDescriptor probeRd;

    boolean roleReversal = false;
    boolean continueToCheckBuckets = false;
    boolean checkForRoleReversal = false;
    ArrayList<IBucket> returnBuckets = new ArrayList<>();

    protected int[] buildKeys;
    protected int[] probeKeys;

    protected byte[] byteArrayForTempBucketTupleR;
    protected byte[] byteArrayForTempBucketTupleS;

    protected ByteBuffer buffForTempBucketTupleR;
    protected ByteBuffer buffForTempBucketTupleS;

    protected IFrameTupleAccessor iFrameTupleAccessorForTempBucketTupleR;
    protected IFrameTupleAccessor iFrameTupleAccessorForTempBucketTupleS;

    public AbstractHeuristic(int memoryForJoin, int frameSize, long buildFileSize, long probeFileSize, RecordDescriptor buildRd,
                    RecordDescriptor probeRd, int[] buildKeys, int[] probeKeys, boolean checkForRoleReversal, boolean continueToCheckBuckets)
            throws HyracksDataException {

        this.memoryForJoinInBytes = memoryForJoin * frameSize;
        this.memoryForJoinInFrames = memoryForJoin;
        this.frameSize = frameSize;
        this.buildFileSize = buildFileSize;
        this.probeFileSize = probeFileSize;
        this.hasNextBuildingBucketSequence = true;
        this.buildRd = buildRd;
        this.probeRd = probeRd;
        this.checkForRoleReversal = checkForRoleReversal;
        this.continueToCheckBuckets = continueToCheckBuckets;

        this.buildKeys = buildKeys;
        this.probeKeys = probeKeys;

        this.byteArrayForTempBucketTupleR = new byte[buildRd.getFieldCount() * 4 + 5 + 5];
        this.byteArrayForTempBucketTupleS = new byte[probeRd.getFieldCount() * 4 + 5 + 5];

        this.buffForTempBucketTupleR = ByteBuffer.wrap(this.byteArrayForTempBucketTupleR);
        this.buffForTempBucketTupleS = ByteBuffer.wrap(this.byteArrayForTempBucketTupleS);

        this.iFrameTupleAccessorForTempBucketTupleR = new FrameTupleAccessor(buildRd);
        this.iFrameTupleAccessorForTempBucketTupleS = new FrameTupleAccessor(probeRd);
    }

    @Override
    public boolean hasNextBuildingBucketSequence() {
        return !bucketsFromR.isEmpty();
    }

    protected void setTupleAccessorForTempBucketTupleR(int bucketId) {
        this.buffForTempBucketTupleR.position(this.buildKeys[0] * 4 + 5 + 4);
        this.buffForTempBucketTupleR.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
        this.buffForTempBucketTupleR.putInt(bucketId);
        this.iFrameTupleAccessorForTempBucketTupleR.reset(this.buffForTempBucketTupleR);
    }

    protected void setTupleAccessorForTempBucketTupleS(int bucketId) {
        this.buffForTempBucketTupleS.position(this.probeKeys[0] * 4 + 5 + 4);
        this.buffForTempBucketTupleS.put(ATypeTag.SERIALIZED_INT32_TYPE_TAG);
        this.buffForTempBucketTupleS.putInt(bucketId);
        this.iFrameTupleAccessorForTempBucketTupleS.reset(this.buffForTempBucketTupleS);
    }

    public void retrieveBuckets(SerializableBucketIdList bucketTable) throws HyracksDataException {

        this.bucketTable = bucketTable;
        this.numberOfBuckets = bucketTable.getNumEntries();
        this.bucketsFromR = new ArrayList<>();
        this.bucketsFromS = new ArrayList<>();
        this.tempBucketsFromR = new ArrayList<>();
        this.tempBucketsFromS = new ArrayList<>();

        realBuildSize = 0;

        //Get buckets from table
        for (int i = 0; i < this.numberOfBuckets; i++) {
            int[] bucket = bucketTable.getEntry(i);
            //skip the missing buckets
            if (bucket[0] == -1) {
                continue;
            }
            //check if the bucket is written to disk and valid & correct the frame index
            if (bucket[1] < 0 && bucket[2] != -1)
                tempBucketsFromR.add(new int[]{bucket[0],-(bucket[1]+1), bucket[2]});
            if(bucket[3] < 0 && bucket[4] != -1)
                tempBucketsFromS.add(new int[]{bucket[0],-(bucket[3]+1), bucket[4]});

        }
        //Sort buckets by their frame index to
        tempBucketsFromR.sort(Comparator.comparingDouble(o -> o[1]));
        tempBucketsFromS.sort(Comparator.comparingDouble(o -> o[1]));

        for (int i = 0; i < tempBucketsFromS.size(); i++) {
            int[] bucket = tempBucketsFromS.get(i);
            int bucketSize;
            int startOffsetInFile;
            int startFrame;
            int startOffset;

            startOffsetInFile = (bucket[1] * this.frameSize) + bucket[2];
            startFrame = bucket[1];
            startOffset = bucket[2];

            int[] nextBucket = new int[3];
            int endFrame;
            int endOffset;

            if (i + 1 < tempBucketsFromS.size()) {
                nextBucket = tempBucketsFromS.get(i + 1);
                endFrame = nextBucket[1];
                endOffset = nextBucket[2];
                bucketSize = ((endFrame * this.frameSize) + endOffset) - startOffsetInFile;
            } else {
                endFrame = -1;
                endOffset = -1;
                bucketSize = (int) ((probeFileSize + 5) - startOffsetInFile);
            }
            //Divide buckets that are bigger than the memory into pieces
            //Note: This part is implemented by assuming every bucket will start from a new frame
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
                    this.bucketsFromS.add(newBucket);
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
                this.bucketsFromS.add(newBucket);
            }
        }

        //Compute the bucket sizes that are matching
        for (int i = 0; i < tempBucketsFromR.size(); i++) {
            boolean matched = false;
            int[] bucket = tempBucketsFromR.get(i);
            setTupleAccessorForTempBucketTupleR(bucket[0]);
            for(int j = 0; j < tempBucketsFromS.size(); j++) {
                setTupleAccessorForTempBucketTupleS(tempBucketsFromS.get(j)[0]);
                if(this.comparator.compare(iFrameTupleAccessorForTempBucketTupleR, 0, iFrameTupleAccessorForTempBucketTupleS, 0) < 1) {
                    matched = true;
                    break;
                }
            }

            if(!matched) continue;

            int bucketSize;
            int startOffsetInFile;
            int startFrame;
            int startOffset;

            startOffsetInFile = (bucket[1] * this.frameSize) + bucket[2];
            startFrame = bucket[1];
            startOffset = bucket[2];

            int[] nextBucket = new int[3];
            int endFrame;
            int endOffset;

            if (i + 1 < tempBucketsFromR.size()) {
                nextBucket = tempBucketsFromR.get(i + 1);
                endFrame = nextBucket[1];
                endOffset = nextBucket[2];
                bucketSize = ((endFrame * this.frameSize) + endOffset) - startOffsetInFile;
            } else {
                endFrame = -1;
                endOffset = -1;
                bucketSize = (int) ((buildFileSize + 5) - startOffsetInFile);
            }
            //Divide buckets that are bigger than the memory into pieces
            //Note: This part is implemented by assuming every bucket will start from a new frame
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
                    this.bucketsFromR.add(newBucket);
                    startFrame += (currentBucketSize / frameSize);
                    tempBucketSize -= memoryForJoinInBytes;
                    realBuildSize += currentBucketSize;
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
                realBuildSize += bucketSize;
            }
        }

    }

    @Override
    public void setComparator(ITuplePairComparator comparator) {
        this.comparator = comparator;
    }

    protected boolean compare() throws HyracksDataException {
        return (this.comparator.compare(iFrameTupleAccessorForTempBucketTupleR, 0, iFrameTupleAccessorForTempBucketTupleS, 0) < 1);
    }
}
