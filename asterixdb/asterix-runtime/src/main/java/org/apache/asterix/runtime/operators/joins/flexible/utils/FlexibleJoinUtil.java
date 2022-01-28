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
package org.apache.asterix.runtime.operators.joins.flexible.utils;

import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.runtime.evaluators.common.SpatialUtils;
import org.apache.asterix.runtime.operators.joins.flexible.utils.memory.FlexibleJoinsUtil;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FlexibleJoinUtil implements IFlexibleJoinUtil {

    protected final int[] idBuild;
    protected final int[] idProbe;

    public FlexibleJoinUtil(int[] idBuild, int[] idProbe) {
        this.idBuild = idBuild;
        this.idProbe = idProbe;
    }

    /**
     * Right (second argument) interval starts before left (first argument) interval ends.
     */
    @Override
    public boolean checkToSaveInMemory(IFrameTupleAccessor buildAccessor, int buildTupleIndex,
            IFrameTupleAccessor probeAccessor, int probeTupleIndex) throws HyracksDataException {
        int buildBucketId = FlexibleJoinsUtil.getBucketId(buildAccessor, buildTupleIndex, idBuild[0]);
        int probeBucketId = FlexibleJoinsUtil.getBucketId(probeAccessor, probeTupleIndex, idProbe[0]);

        return buildBucketId == probeBucketId;
    }

    /**
     * Left (first argument) interval starts after the Right (second argument) interval ends.
     */
    @Override
    public boolean checkToRemoveInMemory(IFrameTupleAccessor buildAccessor, int buildTupleIndex,
            IFrameTupleAccessor probeAccessor, int probeTupleIndex) throws HyracksDataException {
        int buildBucketId = FlexibleJoinsUtil.getBucketId(buildAccessor, buildTupleIndex, idBuild[0]);
        int probeBucketId = FlexibleJoinsUtil.getBucketId(probeAccessor, probeTupleIndex, idProbe[0]);

        return buildBucketId != probeBucketId;
    }

    @Override
    public boolean checkToSaveInResult(IFrameTupleAccessor buildAccessor, int buildTupleIndex,
            IFrameTupleAccessor probeAccessor, int probeTupleIndex) throws HyracksDataException {
        int buildBucketId = FlexibleJoinsUtil.getBucketId(buildAccessor, buildTupleIndex, idBuild[0]);
        int probeBucketId = FlexibleJoinsUtil.getBucketId(probeAccessor, probeTupleIndex, idProbe[0]);

        return buildBucketId == probeBucketId;
    }

    /**
     * Right (second argument) rectangle starts before left (first argument) rectangle ends.
     */
    @Override
    public boolean checkForEarlyExit(IFrameTupleAccessor buildAccessor, int buildTupleIndex,
            IFrameTupleAccessor probeAccessor, int probeTupleIndex) throws HyracksDataException {
        int buildBucketId = FlexibleJoinsUtil.getBucketId(buildAccessor, buildTupleIndex, idBuild[0]);
        int probeBucketId = FlexibleJoinsUtil.getBucketId(probeAccessor, probeTupleIndex, idProbe[0]);

        return buildBucketId != probeBucketId;
    }

    @Override
    public boolean checkToLoadNextProbeTuple(IFrameTupleAccessor buildAccessor, int buildTupleIndex,
            IFrameTupleAccessor probeAccessor, int probeTupleIndex) throws HyracksDataException {
        int buildBucketId = FlexibleJoinsUtil.getBucketId(buildAccessor, buildTupleIndex, idBuild[0]);
        int probeBucketId = FlexibleJoinsUtil.getBucketId(probeAccessor, probeTupleIndex, idProbe[0]);

        return buildBucketId == probeBucketId;
    }
}
