/**
 * Project Name:demo1
 * File Name:LocalClusterTopology.java
 * Package Name:com.oneapm.demo1
 * Date:
 * Copyright (c) 2016, All Rights Reserved.
 *
 */
package com.oneapm.demo1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * ClassName:LocalClusterTopology <br/>
 * Function: <br/>
 * Date: <br/>
 * 
 * @author hadoop
 * @version
 * @since JDK 1.8
 * @see
 */
public class LocalClusterTopology {
    
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
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("demo1",
                               conf,
                               builder.createTopology());
        Utils.sleep(15000);
        cluster.killTopology("demo1");
        cluster.shutdown();
    }
    
}
