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
package org.apache.hyracks.storage.am.common.impls;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.projection.ITupleProjector;

class DefaultTupleProjector implements ITupleProjector {
    public static final ITupleProjector INSTANCE = new DefaultTupleProjector();

    private DefaultTupleProjector() {
    }

    @Override
    public void project(ITupleReference tuple, DataOutput dos, ArrayTupleBuilder tb) throws IOException {
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            dos.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            tb.addFieldEndOffset();
        }
    }
}
