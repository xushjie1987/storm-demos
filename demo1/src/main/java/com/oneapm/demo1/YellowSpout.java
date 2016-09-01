/**
 * Project Name:demo1
 * File Name:YellowSpout.java
 * Package Name:com.oneapm.demo1
 * Date:
 * Copyright (c) 2016, All Rights Reserved.
 *
 */
package com.oneapm.demo1;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClassName:YellowSpout <br/>
 * Function: <br/>
 * Date: <br/>
 * 
 * @author hadoop
 * @version
 * @since JDK 1.8
 * @see
 */
public class YellowSpout extends BaseRichSpout {
    
    private static final Logger  log   = LoggerFactory.getLogger(GreenBolt.class);
    
    private Map                  conf;
    
    private TopologyContext      context;
    
    private SpoutOutputCollector collector;
    
    private int                  count = 0;
    
    /**
     * @see org.apache.storm.spout.ISpout#open(java.util.Map, org.apache.storm.task.TopologyContext,
     *      org.apache.storm.spout.SpoutOutputCollector)
     */
    @Override
    public void open(Map conf,
                     TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        this.conf = conf;
        this.context = context;
    }
    
    /**
     * @see org.apache.storm.spout.ISpout#nextTuple()
     */
    @Override
    public void nextTuple() {
        try {
            if (count <= 20) {
                Thread.sleep(100);
                collector.emit(Utils.DEFAULT_STREAM_ID,
                               new Values(count++,
                                          RandomStringUtils.randomAlphanumeric(10)),
                               UUID.randomUUID()
                                   .toString());
            }
        } catch (InterruptedException e) {
            log.error(e.toString());
        }
    }
    
    /**
     * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Utils.DEFAULT_STREAM_ID,
                               new Fields("id",
                                          "yellow"));
    }
    
    /**
     * @see org.apache.storm.topology.base.BaseRichSpout#ack(java.lang.Object)
     */
    @Override
    public void ack(Object msgId) {
        System.err.println("SUCCESS: " +
                           msgId);
        super.ack(msgId);
    }
    
    /**
     * @see org.apache.storm.topology.base.BaseRichSpout#fail(java.lang.Object)
     */
    @Override
    public void fail(Object msgId) {
        System.err.println("FAILED: " +
                           msgId);
        super.fail(msgId);
    }
    
}
