package io.arabesque.search.steps;

import io.arabesque.search.trees.SearchEmbedding;
import io.arabesque.utils.ThreadOutput;

import java.io.IOException;
import java.util.*;

public abstract class AbstractGenericJoin {
    private ThreadOutput outputStream;
    private Integer numPartition;

    public AbstractGenericJoin(ThreadOutput outputStream, Integer numPartitions){
        this.outputStream = outputStream;
        this.numPartition = numPartitions;
    }

    public abstract long search();

    public void outputResult(Deque<PrefixTuple> q){
        if(outputStream == null)
            return;
        int count = 0;
        Set<String> set = new HashSet<>();
        while (!q.isEmpty()){
            PrefixTuple p = q.poll();
            List<Integer> res = new ArrayList<>(p.map.values());
            Collections.sort(res);
            if(set.add(res.toString())){
                try {
                    SearchEmbedding searchEmbedding = new SearchEmbedding();
                    for(Integer i : res){
                        searchEmbedding.addWord(i);
                    }
                    if(searchEmbedding.getSize() > 0){
                        outputStream.write(searchEmbedding);
                    }
                } catch (IOException e) {
                    System.out.println("Could not write embeddings to Thread Output. Exiting.");
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }

    public Set<Integer> getIntersection(PriorityQueue<Set<Integer>> queue){
        Set<Integer> min = new HashSet<>(queue.poll());
        while (!queue.isEmpty()){
            min.retainAll(queue.poll());
            if(min.size() <= 0)
                break;
        }
        return min;
    }


    public int[] splitIntoParts(int totalNum) {
        int numPartition = this.numPartition;
        int[] arr = new int[numPartition + 1];
        arr[0] = 0;
        arr[numPartition] = totalNum;
        int div = totalNum/numPartition;
        for(int i = 1, currPos = div; i < numPartition; i++, currPos += div){
            arr[i] = currPos;
        }
        return arr;
    }
}
