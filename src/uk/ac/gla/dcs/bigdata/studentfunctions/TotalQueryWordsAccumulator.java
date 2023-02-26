package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.util.AccumulatorV2;

import java.util.HashMap;

public class TotalQueryWordsAccumulator extends AccumulatorV2<HashMap<String, Integer>, HashMap<String, Integer>> {

    private HashMap<String, Integer> hashMap = new HashMap<>();

    @Override
    public boolean isZero() {
        return hashMap.isEmpty();
    }

    @Override
    public AccumulatorV2<HashMap<String, Integer>, HashMap<String, Integer>> copy() {
        TotalQueryWordsAccumulator newAccumulator = new TotalQueryWordsAccumulator();
        newAccumulator.hashMap.putAll(this.hashMap);
        return newAccumulator;
    }

    @Override
    public void reset() {
        hashMap.clear();
    }

    @Override
    public void add(HashMap<String, Integer> hashMapToAdd) {
        for (String key : hashMapToAdd.keySet()) {
            if (hashMap.containsKey(key)) {
                hashMap.put(key, hashMap.get(key) + hashMapToAdd.get(key));
            } else {
                hashMap.put(key, hashMapToAdd.get(key));
            }
        }
    }

    @Override
    public void merge(AccumulatorV2<HashMap<String, Integer>, HashMap<String, Integer>> other) {
        for (String key : other.value().keySet()) {
            if (hashMap.containsKey(key)) {
                hashMap.put(key, hashMap.get(key) + other.value().get(key));
            } else {
                hashMap.put(key, other.value().get(key));
            }
        }
    }

    @Override
    public HashMap<String, Integer> value() {
        return hashMap;
    }
}
