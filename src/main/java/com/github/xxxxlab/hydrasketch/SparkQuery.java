/* (C)2021 */
package com.github.xxxxlab.hydrasketch;

import java.io.*;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkQuery {
    public static void main(String[] args) {
        String expFile = args[0];
        String debug_on = "0";
        String prefix = "1";
        HashMap<String, String> expMap = utils.ReadExp(expFile);
        utils.PrintExp(expMap);
        String config_file = expMap.get("config_file");
        String res_file = expMap.get("res_file");
        String save_spark_file = expMap.get("save_spark_file");
        String sketch_file = expMap.get("sketch_file");
        HashMap<String, Integer> configMap = utils.ReadConfig(config_file);

        int[] key_dim = utils.ParseKeyDim(expMap.get("key_dim"));
        int key_dim_len = key_dim.length;
        int value_dim = Integer.parseInt(expMap.get("value_dim"));
        float value_scale = Float.parseFloat(expMap.get("value_scale"));
        int query_partition = Integer.parseInt(expMap.get("query_partition"));

        SparkConf conf = new SparkConf();
        JavaSparkContext context = new JavaSparkContext(conf);

        ImpHydraStruct res_sketch = PlainQuery.DeserializeSketch(sketch_file);

        // query
        utils.executeBashCommand("aws s3 rm --recursive " + save_spark_file);

        MyTimer query_timer = new MyTimer("query");

        JavaRDD<String> query_lines = context.textFile(res_file, query_partition);
        System.out.printf("partitions %d\n", query_lines.getNumPartitions());

        Boolean DEBUG_Query = true;
        if (DEBUG_Query) {
            String key = "136.28.185.112";
            float[] estimate = res_sketch.queryOneNoCheck(key);
            System.out.printf(
                    "%s,%.2f,%.2f,%.2f,%.2f\n",
                    key, estimate[0], estimate[1], estimate[2], estimate[3]);
        } else {
            JavaRDD<String> est = query_lines.map(new Query(res_sketch, key_dim_len));
            est.saveAsTextFile(save_spark_file);
        }

        query_timer.Stop();

        context.stop();
    }

    static class Query implements Function<String, String> {
        public ImpHydraStruct sketch;
        public int key_dim_len;

        public Query(ImpHydraStruct _sketch, int _key_dim_len) {
            sketch = _sketch;
            key_dim_len = _key_dim_len;
        }

        public String call(String row) throws Exception {
            String[] data = row.split(",", -1);
            String key = utils.makeKey(data, key_dim_len);
            float[] estimate = sketch.queryOneNoCheck(key);
            return String.format(
                    "%s,%.2f,%.2f,%.2f,%.2f",
                    key, estimate[0], estimate[1], estimate[2], estimate[3]);
        }
    }
}
