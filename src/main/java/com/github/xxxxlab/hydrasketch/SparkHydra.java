/* (C)2021 */
package com.github.xxxxlab.hydrasketch;

import java.io.*;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkHydra {
    public static void main(String[] args) {
        String expFile = args[0];
        String debug_on = "0";
        String prefix = "1";
        HashMap<String, String> expMap = utils.ReadExp(expFile);
        utils.PrintExp(expMap);
        String dataset = expMap.get("dataset");
        String input_file = expMap.get("input_file");
        String config_file = expMap.get("config_file");
        String res_file = expMap.get("res_file");
        String save_plain_file = expMap.get("save_plain_file");
        String save_spark_file = expMap.get("save_spark_file");
        String sketch_file = expMap.get("sketch_file");
        HashMap<String, Integer> configMap = utils.ReadConfig(config_file);

        int[] key_dim = utils.ParseKeyDim(expMap.get("key_dim"));
        int key_dim_len = key_dim.length;
        int value_dim = Integer.parseInt(expMap.get("value_dim"));
        int group_cnt = Integer.parseInt(expMap.get("group_cnt"));
        int repeat_ingest_cnt = Integer.parseInt(expMap.get("repeat_ingest_cnt"));
        String repeat_input_file =
                String.join(",", Collections.nCopies(repeat_ingest_cnt, input_file));
        float value_scale = Float.parseFloat(expMap.get("value_scale"));
        int ingest_partition = Integer.parseInt(expMap.get("ingest_partition"));
        int query_partition = Integer.parseInt(expMap.get("query_partition"));

        SparkConf conf = new SparkConf().setAppName("SparkHydra");
        JavaSparkContext context = new JavaSparkContext(conf);

        // update
        MyTimer update_timer = new MyTimer("update");
        System.out.println("start update");
        JavaRDD<String> lines = context.textFile(repeat_input_file, ingest_partition);
        System.out.printf("partitions %d\n", lines.getNumPartitions());
        ImpHydraStruct initialValue = new ImpHydraStruct(configMap, debug_on, prefix);
        ImpHydraStruct res_sketch =
                lines.treeAggregate(
                        initialValue,
                        new SparkIngest.Add(key_dim, value_dim, group_cnt, value_scale, dataset),
                        new SparkIngest.Combine());
        System.out.println("finish update");
        update_timer.Stop();

        try {
            File f = new File(save_plain_file);
            f.delete();
        } catch (Exception e) {
            System.out.println(e);
        }

        System.out.println("start write out");
        SparkIngest.writeOutSketch(res_sketch, sketch_file);

        // query
        Boolean Use_Plain_Query = false;
        System.out.println("start query");
        MyTimer query_timer;

        if (Use_Plain_Query) {
            query_timer = new MyTimer("query");
            PlainQuery.oldQueryBatch(res_sketch, res_file, save_plain_file, key_dim_len);
        } else {
            utils.executeBashCommand("aws s3 rm --recursive " + save_spark_file);

            query_timer = new MyTimer("query");

            JavaRDD<String> query_lines = context.textFile(res_file, query_partition);
            System.out.printf("partitions %d\n", query_lines.getNumPartitions());
            JavaRDD<String> est = query_lines.map(new SparkQuery.Query(res_sketch, key_dim_len));
            est.saveAsTextFile(save_spark_file);
        }

        query_timer.Stop();

        res_sketch.executor.shutdown();
    }
}
