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

import org.apache.asterix.external.cartilage.base.FlexibleJoin;
import org.apache.asterix.external.cartilage.base.Summary;

public class IntervalJoin implements FlexibleJoin<FJInterval, IntervalJoinConfig> {
    private double k = 2;
    public static int matchCounter = 0;
    public static int verifyCounter = 0;

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

        iS1.add(iS2);

        double d1 = (iS1.oEnd - iS1.oStart) / k;
        double d2 = (iS1.oEnd - iS1.oStart) / k;

        this.matchCounter = 0;
        this.verifyCounter = 0;

        return new IntervalJoinConfig(d1, d2, iS1, iS2, k);
    }

    @Override
    public int[] assign1(FJInterval k1, IntervalJoinConfig intervalJoinConfig) {

        int i = (int) ((k1.start - intervalJoinConfig.iS1.oStart) / intervalJoinConfig.d1);
        int j = (int) ((k1.end - intervalJoinConfig.iS1.oStart) / intervalJoinConfig.d1);
        int bucketId = 0;
        for (int s = i; s <= j; s++) {
            bucketId |= 1 << s;
        }
        return new int[] { bucketId };
    }

    /*@Override
    public int[] assign2(FJInterval k1, IntervalJoinConfig intervalJoinConfig) {
    
        int i = (int) ((k1.start - intervalJoinConfig.iS2.oStart) / intervalJoinConfig.d2);
        int j = (int) ((k1.end - intervalJoinConfig.iS2.oStart) / intervalJoinConfig.d2);
        int bucketId = 0;
        for(int s = i; s <= j; s++) {
            bucketId |= 1 << s;
        }
        return new int[] {bucketId};
    }*/

    @Override
    public boolean match(int b1, int b2) {
        matchCounter++;
        int max = Math.max(b1, b2);
        boolean b = max == (b1 | b2);
        if (b)
            verifyCounter++;
        return b;
        //return FlexibleJoin.super.match(b1, b2);
    }

    @Override
    public boolean verify(int b1, FJInterval k1, int b2, FJInterval k2, IntervalJoinConfig c) {
        return verify(k1, k2);
    }

    @Override
    public boolean verify(FJInterval k1, FJInterval k2) {
        return k1.start < k2.end && k1.end > k2.start;
    }

}
