package io.arabesque.search.steps;

import com.koloboke.collect.IntIterator;
import io.arabesque.GJRunner;
import io.arabesque.conf.Configuration;
import io.arabesque.conf.SparkConfiguration;
import io.arabesque.graph.UnsafeCSRGraphSearch;
import io.arabesque.search.trees.Domain;
import io.arabesque.search.trees.SearchDataTree;
import io.arabesque.utils.ThreadOutput;
import io.arabesque.utils.collection.IntArrayList;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.CollectionAccumulator;
import org.mortbay.log.Log;
import scala.Tuple2;

import java.util.*;

import static io.arabesque.search.steps.QueryGraph.STAR_LABEL;
import static io.arabesque.utils.Hashing.murmur3_32;

//import net.openhft.koloboke.collect.IntIterator;

public class GenericJoin
        implements Function2<Integer, Iterator<Tuple2<Integer, Iterable<SearchDataTree>>>, Iterator<Integer>> {

    private static final Logger LOG = Logger.getLogger(GenericJoin.class);

    private final int totalPartitions;
    private Broadcast<SparkConfiguration> configBC;
    private Broadcast<QueryGraph> queryGraphBC;

    // accumulators
    private Map<String, CollectionAccumulator<Long>> accums;
    private long init_Start_Time = 0;
    private long init_Finish_Time = 0;
    private long computation_Start_Time = 0;
    private long computation_Finish_Time = 0;

    public GenericJoin(int totalPartitions, Broadcast<SparkConfiguration> configBC, Broadcast<QueryGraph> queryGraphBC, Map<String, CollectionAccumulator<Long>> _accums) {
        super();

        init_Start_Time = System.currentTimeMillis();

        configBC.value().initialize();
        Configuration conf = Configuration.get();

        String log_level = conf.getLogLevel();
        LOG.fatal("Setting log level to " + log_level);
        LOG.setLevel(Level.toLevel(log_level));

        this.totalPartitions = totalPartitions;
        this.configBC = configBC;
        this.queryGraphBC = queryGraphBC;

        this.accums = _accums;
    }

    @Override
    public Iterator<Integer> call(Integer partitionId, Iterator<Tuple2<Integer, Iterable<SearchDataTree>>> v2) throws Exception {
        System.out.println("Current partition id is: " + partitionId);
        Configuration conf = configBC.value();
        conf.initialize();

        UnsafeCSRGraphSearch dataGraph = Configuration.get().getSearchMainGraph();
        QueryGraph queryGraph = queryGraphBC.getValue();

        // get the initialization finish time stamp
        init_Finish_Time = System.currentTimeMillis();
        // get the computation runtime stamp
        computation_Start_Time = System.currentTimeMillis();

        ThreadOutput out = ThreadOutputHandler.createThreadOutput(partitionId, false);

        AbstractGenericJoin gj = new GJLabelled(queryGraph, partitionId, totalPartitions, dataGraph, out);
        long result = gj.search();
        // finished all the work we have locally
        ThreadOutputHandler.closeThreadOutput(out);
        flushAccumulators(result);

        // TODO we actually don't need to output an RDD. All matches are stored to HDFS.
        ArrayList<Integer> list = new ArrayList();
        list.add(0);
        flushAccumulators(result);

        return list.iterator();
    }

    private void accumulate(Long value, CollectionAccumulator<Long> _accum) {
        _accum.add( value );
    }

    private void flushAccumulators(long result) {
        // Now calculate the computation runtime
        computation_Finish_Time = System.currentTimeMillis();

        accumulate(init_Start_Time, accums.get(GJRunner.TREE_BUILDING_INIT_START_TIME));
        accumulate(init_Finish_Time, accums.get(GJRunner.TREE_BUILDING_INIT_FINISH_TIME));
        accumulate(computation_Start_Time, accums.get(GJRunner.TREE_BUILDING_COMPUTATION_START_TIME));
        accumulate(computation_Finish_Time, accums.get(GJRunner.TREE_BUILDING_COMPUTATION_FINISH_TIME));
        accumulate(result, accums.get(GJRunner.COUNT_RESULTS));
    }
}
