package io.arabesque.search.steps;

import java.util.*;

public class PrefixTuple {
    Map<Integer, Integer> map = null;

    public PrefixTuple(){
        this.map = new HashMap<>();
    }

    public void addToMap(int label, int vertex){
        this.map.put(label, vertex);
    }
}
