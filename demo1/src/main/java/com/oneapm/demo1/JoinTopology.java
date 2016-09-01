/**
 * Project Name:demo1
 * File Name:JoinTopology.java
 * Package Name:com.oneapm.demo1
 * Date:
 * Copyright (c) 2016, All Rights Reserved.
 *
 */
package com.oneapm.demo1;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * ClassName:JoinTopology <br/>
 * Function: <br/>
 * Date: <br/>
 * 
 * @author hadoop
 * @version
 * @since JDK 1.8
 * @see
 */
public class JoinTopology {
    
    /**
     * main: <br/>
     * 
     * @author hadoop
     * @param args
     * @throws AuthorizationException
     * @throws InvalidTopologyException
     * @throws AlreadyAliveException
     * @since JDK 1.8
     */
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        //
        YellowSpout yellow = new YellowSpout();
        RedSpout red = new RedSpout();
        //
        GreenBolt green = new GreenBolt();
        //
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("yellow",
                         yellow,
                         1);
        builder.setSpout("red",
                         red,
                         1);
        builder.setBolt("green",
                        green,
                        2)
               .setNumTasks(4)
               .fieldsGrouping("yellow",
                               Utils.DEFAULT_STREAM_ID,
                               new Fields("id"))
               .fieldsGrouping("red",
                               Utils.DEFAULT_STREAM_ID,
                               new Fields("id"));
        //
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);
        //
        StormSubmitter.submitTopologyWithProgressBar("demo1",
                                                     conf,
                                                     builder.createTopology());
        
    }
    
}
