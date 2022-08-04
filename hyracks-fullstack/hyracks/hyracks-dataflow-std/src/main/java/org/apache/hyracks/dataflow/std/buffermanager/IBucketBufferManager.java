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

package org.apache.hyracks.dataflow.std.buffermanager;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public interface IBucketBufferManager {

    int getNumTuples();

    int getPhysicalSize();

    /**
     * Insert tuple {@code tupleId} from the {@code tupleAccessor} into the given partition.
     * The returned handle is written into the tuplepointer
     * @param tupleAccessor
     *            the FrameTupleAccessor storage
     * @param tupleId
     *            the id of the tuple from the tupleAccessor
     * @param pointer
     *            the returned pointer indicating the handler to later fetch the tuple from the buffer maanager
     * @return true if the insertion succeed. Otherwise return false.
     * @throws HyracksDataException
     */
    boolean insertTuple(IFrameTupleAccessor tupleAccessor, int tupleId, TuplePointer pointer)
            throws HyracksDataException;

    /**
     * Sets the constrain.
     * @param constrain
     *              the constrain to be set.
     */
    void setConstrain(IPartitionedMemoryConstrain constrain);

    /**
     * Reset to the initial states. The previous allocated resources won't be released in order to be used in the next round.
     *
     * @throws HyracksDataException
     */
    void reset() throws HyracksDataException;

    /**
     * Close the managers which will explicitly release all the allocated resources.
     */
    void close();

    ITuplePointerAccessor getTuplePointerAccessor(RecordDescriptor recordDescriptor);

    IPartitionedMemoryConstrain getConstrain();
}
