package org.apache.asterix.runtime.flexiblejoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class WordCount implements Summary<String> {

    public Map<String, Integer> WordCountMap = new HashMap<>();

    @Override
    public void add(String k) {
        ArrayList<String> tokens = Utilities.tokenizer(k);
        for (String token : tokens) {
            WordCountMap.merge(token, 1, Integer::sum);
        }
    }

    @Override
    public void add(Summary<String> s) {
        WordCount wc = (WordCount) s;
        for (String token : WordCountMap.keySet()) {
            WordCountMap.merge(token, wc.WordCountMap.get(token), Integer::sum);
        }
    }
}
