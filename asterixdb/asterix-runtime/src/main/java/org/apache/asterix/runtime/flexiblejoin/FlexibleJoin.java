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
package org.apache.asterix.runtime.flexiblejoin;

public interface FlexibleJoin<T, C> {
    Summary<T> createSummarizer1();

    default Summary<T> createSummarizer2() {
        return createSummarizer1();
    }

    C divide(Summary<T> s1, Summary<T> s2);

    int[] assign1(T k1, C c);

    default int[] assign2(T k2, C c) {
        return assign1(k2, c);
    }

    default boolean match(int b1, int b2) {
        return b1 == b2;
    }

    boolean verify(int b1, T k1, int b2, T k2, C c);
}
