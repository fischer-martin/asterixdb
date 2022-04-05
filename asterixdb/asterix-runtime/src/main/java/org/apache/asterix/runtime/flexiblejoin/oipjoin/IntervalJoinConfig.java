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
package org.apache.asterix.runtime.flexiblejoin.oipjoin;

import org.apache.asterix.runtime.flexiblejoin.cartilage.Configuration;

public class IntervalJoinConfig implements Configuration {
    public intervalSummary iS1;
    public intervalSummary iS2;

    double k;

    double d1;
    double d2;

    IntervalJoinConfig(double d1, double d2, intervalSummary iS1, intervalSummary iS2, double k) {
        this.iS1 = iS1;
        this.iS2 = iS2;

        this.d1 = d1;
        this.d2 = d2;

        this.k = k;
    }
}
