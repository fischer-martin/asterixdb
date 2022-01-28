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
package org.apache.asterix.runtime.operators.joins.flexible.utils.memory;

import org.apache.asterix.runtime.operators.joins.flexible.utils.IFlexibleJoinUtil;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.ITupleCursor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FlexibleJoinsSideTuple {
    // Tuple access
    int[] fieldId;
    ITupleCursor cursor;

    // Join details
    final IFlexibleJoinUtil imjc;

    public FlexibleJoinsSideTuple(IFlexibleJoinUtil imjc, ITupleCursor cursor, int[] fieldId) {
        this.imjc = imjc;
        this.cursor = cursor;
        this.fieldId = fieldId;
    }

    public int getTupleIndex() {
        return cursor.getTupleId();
    }

    public ITupleCursor getCursor() {
        return cursor;
    }

    public boolean compareJoin(FlexibleJoinsSideTuple ist) throws HyracksDataException {
        return imjc.checkToSaveInResult(cursor.getAccessor(), cursor.getTupleId(), ist.cursor.getAccessor(),
                ist.cursor.getTupleId());
    }

    public boolean removeFromMemory(FlexibleJoinsSideTuple ist) throws HyracksDataException {
        return imjc.checkToRemoveInMemory(cursor.getAccessor(), cursor.getTupleId(), ist.cursor.getAccessor(),
                ist.cursor.getTupleId());
    }

    public boolean checkForEarlyExit(FlexibleJoinsSideTuple ist) throws HyracksDataException {
        return imjc.checkForEarlyExit(cursor.getAccessor(), cursor.getTupleId(), ist.cursor.getAccessor(),
                ist.cursor.getTupleId());
    }
}
