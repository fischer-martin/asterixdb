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
package org.apache.asterix.external.cartilage.setsimilarity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.external.cartilage.base.FlexibleJoin;
import org.apache.asterix.external.cartilage.base.Summary;
import org.apache.commons.text.similarity.JaccardSimilarity;

public class SetSimilarityJoin implements FlexibleJoin<String, SetSimilarityConfig> {
    Double SimilarityThreshold = 0.0;
    public static int verifyCounter = 0;
    public static int matchCounter = 0;

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
        /*int startIx = 0;
        int l = k1.length();
        k1 = k1.toLowerCase();
        
        ArrayList<Integer> ranks = new ArrayList<>();
        int length = 0;
        while (startIx < l) {
            while (startIx < l && isSeparator(k1.charAt(startIx))) {
                startIx++;
            }
            int tokenStart = startIx;
        
            while (startIx < l && !isSeparator(k1.charAt(startIx))) {
                startIx++;
            }
            int tokenEnd = startIx;
        
            // Emit token.
        
            String token = k1.substring(tokenStart, tokenEnd);
            if(!token.isEmpty()) {
                ranks.add(setSimilarityConfig.S.get(token));
                length++;
            }
        
        }
        
        int PrefixLength = (int) (length - Math.ceil(SimilarityThreshold * length) + 1);
        
        int[] ranksToReturn = new int[PrefixLength];
        Collections.sort(ranks);
        for (int i = 0; i < PrefixLength; i++) {
            ranksToReturn[i] = ranks.get(i);
        }
        return ranksToReturn;
        
        String[] tokens = Utilities.tokenizer(k1);
        int length = tokens.length;
        int[] ranksToReturn = new int[length];
        int PrefixLength = (int) (length - Math.ceil(SimilarityThreshold * length) + 1);
        for (int i = 0; i < PrefixLength; i++) {
            int rank = setSimilarityConfig.S.getOrDefault(tokens[i], 0);
            ranksToReturn[i] = rank;
        }
        Arrays.sort(ranksToReturn);
        return Arrays.copyOf(ranksToReturn, PrefixLength);
        */
        String[] tokens = Utilities.tokenizer(k1);
        int length = tokens.length;
        int PrefixLength = (int) (length - Math.ceil(SimilarityThreshold * length) + 1);
        ArrayList<Integer> ranks = new ArrayList<>();
        for (String token : tokens) {
            int rank = setSimilarityConfig.S.get(token);
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
        //matchCounter++;
        return FlexibleJoin.super.match(b1, b2);
    }

    @Override
    public boolean verify(String k1, String k2) {
        //String[] leftTokens = tokenizer(k1);
        //String[] rightTokens = tokenizer(k2);

        int leftLength = k1.length();
        int rightLength = k2.length();

        // apply length filter
        int lengthLowerBound = (int) Math.ceil(SimilarityThreshold * leftLength);

        boolean passesLengthFilter =
                (lengthLowerBound <= rightLength) && (rightLength <= 1.0f / SimilarityThreshold * leftLength);
        if (!passesLengthFilter) {
            return false;
        }

        return calculateJaccardSimilarityHashMap(k1, k2) >= SimilarityThreshold;

    }

    public static double calculateJaccardSimilaritySorted(String[] left, String[] right) {

        double intersectionSize = 0;

        int leftLength = left.length;
        int rightLength = right.length;

        Arrays.sort(left);
        Arrays.sort(right);

        int leftIndex = 0;
        int rightIndex = 0;
        while (leftIndex < leftLength && rightIndex < rightLength) {
            if (left[leftIndex].equals(right[rightIndex])) {
                leftIndex++;
                rightIndex++;
                intersectionSize++;
            } else if (left[leftIndex].compareTo(right[rightIndex]) > 0) {
                rightIndex++;
            } else {
                leftIndex++;
            }
        }
        double sim = (intersectionSize / ((leftLength + rightLength) - intersectionSize));
        sim = Math.round(sim * 100000000d) / 100000000d;
        return sim;
    }

    public static double calculateJaccardSimilarityHashMap(String left, String right) {

        double intersectionSize = 0;

        int leftLength = left.length();
        int rightLength = right.length();

        int leftTokenC = 0;
        int rightTokenC = 0;

        HashMap<String, Integer> map = new HashMap<>();

        String probe;
        String build;

        if (leftLength < rightLength) {
            build = left.toLowerCase();
            probe = right.toLowerCase();
        } else {
            build = right.toLowerCase();
            probe = left.toLowerCase();
        }

        int startIx = 0;
        int l = build.length();

        // Skip separators at beginning of string.

        while (startIx < l) {
            while (startIx < l && isSeparator(build.charAt(startIx))) {
                startIx++;
            }
            int tokenStart = startIx;

            while (startIx < l && !isSeparator(build.charAt(startIx))) {
                startIx++;
            }
            int tokenEnd = startIx;

            // Emit token.
            String token = build.substring(tokenStart, tokenEnd);
            if (!token.isEmpty()) {
                map.merge(token, 1, Integer::sum);
                leftTokenC++;
            }
        }

        startIx = 0;
        l = probe.length();

        // Skip separators at beginning of string.

        while (startIx < l) {
            while (startIx < l && isSeparator(probe.charAt(startIx))) {
                startIx++;
            }
            int tokenStart = startIx;

            while (startIx < l && !isSeparator(probe.charAt(startIx))) {
                startIx++;
            }
            int tokenEnd = startIx;

            // Emit token.
            String token = probe.substring(tokenStart, tokenEnd);
            if (!token.isEmpty()) {
                if (map.containsKey(token)) {
                    map.merge(token, -1, Integer::sum);
                    if (map.get(token) == 0)
                        map.remove(token);
                    intersectionSize++;
                }
                rightTokenC++;
            }
        }

        double sim = (intersectionSize / ((leftTokenC + rightTokenC) - intersectionSize));
        sim = Math.round(sim * 100000000d) / 100000000d;
        return sim;
    }

    public static double calculateJaccardSimilarityHashMap(String[] left, String[] right) {

        double intersectionSize = 0;

        int leftLength = left.length;
        int rightLength = right.length;

        HashMap<String, Integer> map = new HashMap<>();

        String[] probe = null;
        String[] build = null;
        if (leftLength < rightLength) {
            build = left;
            probe = right;
        } else {
            build = right;
            probe = left;
        }

        for (String s : build) {
            map.merge(s, 1, Integer::sum);
        }
        for (String s : probe) {
            if (map.containsKey(s)) {
                map.merge(s, -1, Integer::sum);
                if (map.get(s) == 0)
                    map.remove(s);
                intersectionSize++;
            }
        }

        double sim = (intersectionSize / ((leftLength + rightLength) - intersectionSize));
        sim = Math.round(sim * 100000000d) / 100000000d;
        return sim;
    }

    public static String[] tokenizer(String text) {
        ArrayList<String> tokens = new ArrayList<>();
        String lowerCaseText = text.toLowerCase();
        int startIx = 0;
        int l = lowerCaseText.length();

        // Skip separators at beginning of string.

        while (startIx < l) {
            while (startIx < l && isSeparator(lowerCaseText.charAt(startIx))) {
                startIx++;
            }
            int tokenStart = startIx;

            while (startIx < l && !isSeparator(lowerCaseText.charAt(startIx))) {
                startIx++;
            }
            int tokenEnd = startIx;

            // Emit token.
            String token = lowerCaseText.substring(tokenStart, tokenEnd);
            tokens.add(token);
        }
        String[] arr = new String[tokens.size()];
        arr = tokens.toArray(arr);
        return arr;
    }

    private static boolean isSeparator(char c) {
        return !(Character.isLetterOrDigit(c) || Character.getType(c) == Character.OTHER_LETTER
                || Character.getType(c) == Character.OTHER_NUMBER);
    }

}

abstract class Utilities {
    private static final JaccardSimilarity js = new JaccardSimilarity();

    public static Double cjs(CharSequence left, CharSequence right) {
        return js.apply(left, right);
    }

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

    public static String[] tokenizer(String text) {
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
        String[] arr = new String[tokens.size()];
        arr = tokens.toArray(arr);
        return arr;
    }

    private static boolean isSeparator(char c) {
        return !(Character.isLetterOrDigit(c) || Character.getType(c) == Character.OTHER_LETTER
                || Character.getType(c) == Character.OTHER_NUMBER);
    }
}
