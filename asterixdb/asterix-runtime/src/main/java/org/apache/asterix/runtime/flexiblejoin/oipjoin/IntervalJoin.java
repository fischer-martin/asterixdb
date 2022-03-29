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

import org.apache.asterix.runtime.flexiblejoin.cartilage.FlexibleJoin;
import org.apache.asterix.runtime.flexiblejoin.cartilage.Summary;
import org.apache.asterix.runtime.flexiblejoin.setsimilarity.WordCount;

public class IntervalJoin implements FlexibleJoin<FJInterval, IntervalJoinConfig> {
    private double k = 2;

    public IntervalJoin(double k) {
        this.k = k;
    }

    @Override
    public Summary<FJInterval> createSummarizer1() {
        return new intervalSummary();
    }

    @Override
    public IntervalJoinConfig divide(Summary<FJInterval> s1, Summary<FJInterval> s2) {

        intervalSummary iS1 = (intervalSummary) s1;
        intervalSummary iS2 = (intervalSummary) s2;

        double d1 = (iS1.oStart - iS1.oEnd) / k;
        double d2 = (iS2.oStart - iS2.oEnd) / k;

        return new IntervalJoinConfig(d1, d2, iS1, iS2, k);
    }

    @Override
    public int[] assign1(FJInterval k1, IntervalJoinConfig intervalJoinConfig) {
        return new int[] {0};
    }

    @Override
    public boolean match(int b1, int b2) {
        return FlexibleJoin.super.match(b1, b2);
    }

    @Override
    public boolean verify(FJInterval k1, FJInterval k2) {
        return false;
    }

}
