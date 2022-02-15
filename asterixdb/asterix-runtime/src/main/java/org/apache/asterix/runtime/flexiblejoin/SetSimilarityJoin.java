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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SetSimilarityJoin implements FlexibleJoin<String, SetSimilarityConfig> {
    Double SimilarityThreshold = 0.0;

    public SetSimilarityJoin(Double SimilarityThreshold) {
        this.SimilarityThreshold = SimilarityThreshold;
    }

    @Override
    public Summary<String> createSummarizer1() {
        return new WordCount();
    }

    @Override
    public SetSimilarityConfig divide(Summary<String> s1, Summary<String> s2) {
        WordCount s1wc = (WordCount) s1;
        WordCount s2wc = (WordCount) s2;
        for (String token : s1wc.WordCountMap.keySet()) {
            s2wc.WordCountMap.merge(token, s1wc.WordCountMap.get(token), Integer::sum);
        }

        LinkedHashMap<String, Integer> SortedWordCountMap =
                s2wc.WordCountMap.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        return new SetSimilarityConfig(SortedWordCountMap.keySet().toArray(String[]::new));

    }

    @Override
    public int[] assign1(String k1, SetSimilarityConfig setSimilarityConfig) {
        ArrayList<String> tokens = Utilities.tokenizer(k1);
        int length = tokens.size();
        int PrefixLength = (int) (length - Math.ceil(SimilarityThreshold * length) + 1);
        ArrayList<Integer> ranks = new ArrayList<>();
        for (String token : tokens) {
            int rank = setSimilarityConfig.S.get(token);
            if (!ranks.contains(rank))
                ranks.add(rank);
        }
        int[] ranksToReturn = new int[PrefixLength];
        Collections.sort(ranks);
        for (int i = 0; i < PrefixLength; i++) {
            if (i >= ranks.size())
                break;
            ranksToReturn[i] = ranks.get(i);

        }
        return ranksToReturn;
    }

    @Override
    public boolean match(int b1, int b2) {
        return FlexibleJoin.super.match(b1, b2);
    }

    @Override
    public boolean verify(String k1, String k2) {
        return Utilities.calculateJaccardSimilarity(k1, k2) >= SimilarityThreshold;
    }

}

abstract class Utilities {
    public static Double calculateJaccardSimilarity(CharSequence left, CharSequence right) {
        Set<String> intersectionSet = new HashSet<String>();
        Set<String> unionSet = new HashSet<String>();
        boolean unionFilled = false;
        int leftLength = left.length();
        int rightLength = right.length();
        if (leftLength == 0 || rightLength == 0) {
            return 0d;
        }

        for (int leftIndex = 0; leftIndex < leftLength; leftIndex++) {
            unionSet.add(String.valueOf(left.charAt(leftIndex)));
            for (int rightIndex = 0; rightIndex < rightLength; rightIndex++) {
                if (!unionFilled) {
                    unionSet.add(String.valueOf(right.charAt(rightIndex)));
                }
                if (left.charAt(leftIndex) == right.charAt(rightIndex)) {
                    intersectionSet.add(String.valueOf(left.charAt(leftIndex)));
                }
            }
            unionFilled = true;
        }
        return (Double.valueOf(intersectionSet.size()) / Double.valueOf(unionSet.size()));
    }

    public static double calculateJaccardSimilarityS(String left, String right) {

        double intersectionSize = 0;
        ArrayList<String> leftTokens = tokenizer(left);
        ArrayList<String> rightTokens = tokenizer(right);

        int leftLength = leftTokens.size();
        int rightLength = rightTokens.size();
        if (leftLength == 0 || rightLength == 0) {
            return 0f;
        }

        for (int leftIndex = 0; leftIndex < leftLength; leftIndex++) {
            int i = 0;
            for (String rt : rightTokens) {
                if (leftTokens.get(leftIndex).equals(rt)) {
                    intersectionSize = intersectionSize + 1.0f;
                    rightTokens.remove(i);
                    break;
                }
                i++;
            }
        }
        double sim = (intersectionSize / ((leftLength + rightLength) - intersectionSize));
        sim = Math.round(sim * 100000000d) / 100000000d;
        return sim;
    }

    public static ArrayList<String> tokenizer(String text) {
        ArrayList<String> tokens = new ArrayList<>();
        String lowerCaseText = text.toLowerCase();
        int startIx = 0;

        while (startIx < lowerCaseText.length()) {
            while (startIx < lowerCaseText.length() && isSeparator(lowerCaseText.charAt(startIx))) {
                startIx++;
            }
            int tokenStart = startIx;

            while (startIx < lowerCaseText.length() && !isSeparator(lowerCaseText.charAt(startIx))) {
                startIx++;
            }
            int tokenEnd = startIx;

            String token = lowerCaseText.substring(tokenStart, tokenEnd);

            if (!token.isEmpty())
                tokens.add(token);
        }
        return tokens;
    }

    private static boolean isSeparator(char c) {
        return !(Character.isLetterOrDigit(c) || Character.getType(c) == Character.OTHER_LETTER
                || Character.getType(c) == Character.OTHER_NUMBER);
    }
}
