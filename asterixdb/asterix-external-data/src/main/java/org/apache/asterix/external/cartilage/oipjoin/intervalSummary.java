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
package org.apache.asterix.external.cartilage.oipjoin;

import org.apache.asterix.external.cartilage.base.Summary;

public class intervalSummary implements Summary<FJInterval> {
    public long oStart = Long.MAX_VALUE;
    public long oEnd = Long.MIN_VALUE;

    @Override
    public void add(FJInterval k) {
        if (k.start < this.oStart)
            this.oStart = k.start;
        if (k.end > this.oEnd)
            this.oEnd = k.end;
    }

    @Override
    public void add(Summary<FJInterval> s) {
        intervalSummary iS = (intervalSummary) s;

        if (iS.oStart < this.oStart)
            this.oStart = iS.oStart;
        if (iS.oEnd > this.oEnd)
            this.oEnd = iS.oEnd;
    }
}
