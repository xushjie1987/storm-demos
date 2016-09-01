/**
 * Project Name:demo1
 * File Name:GreenBolt.java
 * Package Name:com.oneapm.demo1
 * Date:
 * Copyright (c) 2016, All Rights Reserved.
 *
 */
package com.oneapm.demo1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClassName:GreenBolt <br/>
 * Function: <br/>
 * Date: <br/>
 * 
 * @author hadoop
 * @version
 * @since JDK 1.8
 * @see
 */
public class GreenBolt extends BaseRichBolt {
    
    private static final Logger                            log  = LoggerFactory.getLogger(GreenBolt.class);
    
    private Map                                            stormConf;
    
    private TopologyContext                                context;
    
    private OutputCollector                                collector;
    
    private ConcurrentHashMap<Integer, Pair<Tuple, Tuple>> join = new ConcurrentHashMap<Integer, Pair<Tuple, Tuple>>();
    
    /**
     * @see org.apache.storm.task.IBolt#prepare(java.util.Map,
     *      org.apache.storm.task.TopologyContext, org.apache.storm.task.OutputCollector)
     */
    @Override
    public void prepare(Map stormConf,
                        TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.stormConf = stormConf;
        this.context = context;
    }
    
    /**
     * @see org.apache.storm.task.IBolt#execute(org.apache.storm.tuple.Tuple)
     */
    @Override
    public void execute(Tuple input) {
        final Optional<MutablePair<Tuple, Tuple>> j = joinTuples(input);
        j.ifPresent(pair -> {
            String result = j.get()
                             .getLeft()
                             .getStringByField("yellow") +
                            "--->>>" +
                            j.get()
                             .getRight()
                             .getStringByField("red");
            System.err.println(result);
            List<Tuple> ts = new ArrayList<Tuple>();
            ts.add(j.get()
                    .getLeft());
            ts.add(j.get()
                    .getRight());
            collector.emit(Utils.DEFAULT_STREAM_ID,
                           ts,
                           new Values(input.getIntegerByField("id"),
                                      result));
            collector.ack(j.get()
                           .getLeft());
            collector.ack(j.get()
                           .getRight());
        });
    }
    
    /**
     * joinTuples: <br/>
     * 
     * @author hadoop
     * @param t
     * @return
     * @since JDK 1.8
     */
    private synchronized Optional<MutablePair<Tuple, Tuple>> joinTuples(Tuple t) {
        MutablePair<Tuple, Tuple> p = (MutablePair<Tuple, Tuple>) join.getOrDefault(t.getIntegerByField("id"),
                                                                                    new MutablePair<Tuple, Tuple>());
        if (t.contains("yellow")) {
            p.setLeft(t);
        }
        if (t.contains("red")) {
            p.setRight(t);
        }
        join.putIfAbsent(t.getIntegerByField("id"),
                         p);
        if (p.getLeft() == null ||
            p.getRight() == null) {
            return Optional.empty();
        }
        join.remove(t.getIntegerByField("id"));
        return Optional.ofNullable(p);
    }
    
    /**
     * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Utils.DEFAULT_STREAM_ID,
                               new Fields("id",
                                          "join"));
    }
    
}
