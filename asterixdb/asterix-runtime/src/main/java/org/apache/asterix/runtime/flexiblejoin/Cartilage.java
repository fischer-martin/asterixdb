package org.apache.asterix.runtime.flexiblejoin;/*
 * Copyright 2021 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

interface Configuration {}

interface FlexibleJoin<T, C> {
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

class Pair<K, V> {
  K k;
  V v;

  public Pair(K k, V v) {
    this.k = k;
    this.v = v;
  }
}

public class Cartilage {
  <K, C extends Configuration> List<Pair<K, K>> flexibleJoin(List<K> r, List<K> s, FlexibleJoin<K, C> joiner) {
    // Summarize the two datasets
    Summary<K> summary1 = joiner.createSummarizer1();
    for (K k1 : r)
      summary1.add(k1);

    Summary<K> summary2 = joiner.createSummarizer2();
    for (K k2 : s)
      summary2.add(k2);

    // Create the partitioning configuration
    C c = joiner.divide(summary1, summary2);

    // Partition both datasets
    Map<Integer, List<K>> buckets1 = new HashMap<>();
    for (K k1: r) {
      int[] buckets = joiner.assign1(k1, c);
      for (int b1 : buckets) {
        List<K> bucket1 = buckets1.get(b1);
        if (bucket1 == null) {
          bucket1 = new ArrayList<>();
          buckets1.put(b1, bucket1);
        }
        bucket1.add(k1);
      }
    }

    Map<Integer, List<K>> buckets2 = new HashMap<>();
    for (K k2: s) {
      int[] buckets = joiner.assign2(k2, c);
      for (int b2 : buckets) {
        List<K> bucket2 = buckets2.get(b2);
        if (bucket2 == null) {
          bucket2 = new ArrayList<>();
          buckets2.put(b2, bucket2);
        }
        bucket2.add(k2);
      }
    }



    List<Pair<K, K>> results = new ArrayList<>();
    // Join all matching buckets
    for (Map.Entry<Integer, List<K>> bucket1 : buckets1.entrySet()) {
      for (Map.Entry<Integer, List<K>> bucket2 : buckets2.entrySet()) {
        if (joiner.match(bucket1.getKey(), bucket2.getKey())) {
          // Join all records in the matching buckets
          for (K k1 : bucket1.getValue()) {
            for (K k2 : bucket2.getValue()) {
              if (joiner.verify(bucket1.getKey(), k1, bucket2.getKey(), k2, c)) {
                //Duplicate avoidance
                int[] buckets1DA = joiner.assign1(k1, c);
                int[] buckets2DA = joiner.assign2(k2, c);

                Arrays.sort(buckets1DA);
                Arrays.sort(buckets2DA);

                boolean stop = false;
                for(int b1:buckets1DA) {
                  for(int b2:buckets2DA) {

                    if(joiner.match(b1, b2)) {
                      if(b1 == bucket1.getKey() && b2 == bucket2.getKey()) {
                          results.add(new Pair<>(k1, k2));
                      }
                      stop = true;
                      break;
                    }
                  }
                  if(stop) break;
                }

              }
            }
          }
        }
      }
    }
    return results;
  }
}
