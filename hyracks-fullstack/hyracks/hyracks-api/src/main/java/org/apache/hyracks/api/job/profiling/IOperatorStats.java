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
package org.apache.hyracks.api.job.profiling;

import java.io.Serializable;

import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.io.IWritable;
import org.apache.hyracks.api.job.profiling.counters.ICounter;

public interface IOperatorStats extends IWritable, Serializable {

    /**
     * @return The name of the operator
     */
    String getName();

    /**
     * @return A counter used to track the number of tuples
     * accessed by an operator
     */
    ICounter getTupleCounter();

    /**
     * @return A counter used to track the execution time
     * of an operator
     */
    ICounter getTimeCounter();

    /**
     * @return A counter used to track the number of pages pinned by an operator
     */
    ICounter getPageReads();

    /**
     * @return A counter used to track the number of pages read from disk by an operator
     */

    ICounter coldReadCounter();

    /**
     * @return A counter used to set the average tuple size outputted by an operator
     */

    ICounter getAverageTupleSz();

    /**
     * @return A counter used to set the max tuple size outputted by an operator
     */

    ICounter getMaxTupleSz();

    /**
     * @return A counter used to set the min tuple size outputted by an operator
     */

    ICounter getMinTupleSz();

    /**
     * @return A counter used to track the number of tuples read by operators that originate data,
     *         like index searches or other scan types
     */

    ICounter getInputTupleCounter();

    ICounter getLevel();

    ICounter getBytesRead();

    ICounter getBytesWritten();

    OperatorDescriptorId getId();
}
