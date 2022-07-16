/* (C)2021 */
package com.github.xxxxlab.hydrasketch;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import java.io.*;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class SparkIngest {
    public static void main(String[] args) {
        String expFile = args[0];
        String debug_on = "0";
        String prefix = "1";
        HashMap<String, String> expMap = utils.ReadExp(expFile);
        utils.PrintExp(expMap);
        String dataset = expMap.get("dataset");
        String input_file = expMap.get("input_file");
        String config_file = expMap.get("config_file");
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

        SparkConf conf = new SparkConf().setAppName("SparkIngest");
        JavaSparkContext context = new JavaSparkContext(conf);

        // update
        MyTimer update_timer = new MyTimer("update");
        System.out.println("start update");
        JavaRDD<String> lines = context.textFile(repeat_input_file, ingest_partition);
        System.out.printf("partitions %d\n", lines.getNumPartitions());
        ImpHydraStruct initialValue = new ImpHydraStruct(configMap, debug_on, prefix);
        ImpHydraStruct res_sketch =
                lines.aggregate(
                        initialValue,
                        new Add(key_dim, value_dim, group_cnt, value_scale, dataset),
                        new Combine());
        System.out.println("finish update");
        update_timer.Stop();

        writeOutSketch(res_sketch, sketch_file);
    }

    static void writeOutSketch(ImpHydraStruct sketch, String sketch_file) {
        try {
            MyTimer serialize_timer = new MyTimer("serialize");
            FileOutputStream fileOut = new FileOutputStream(sketch_file);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(sketch);
            out.close();
            fileOut.close();
            serialize_timer.Stop();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    static void writeOutSketchKryo(ImpHydraStruct sketch, String sketch_file) {
        try {
            MyTimer serialize_timer = new MyTimer("serialize");
            FileOutputStream fileOut = new FileOutputStream(sketch_file);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            Output output = new Output(out);
            Kryo kryo = new Kryo();
            kryo.register(ImpHydraStruct.class);
            kryo.writeObject(output, sketch);
            output.close();
            out.close();
            fileOut.close();
            serialize_timer.Stop();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    static class Add implements Function2<ImpHydraStruct, String, ImpHydraStruct> {
        public int[] key_dim;
        public int value_dim;
        public final int group_cnt;
        public final float value_scale;
        public final String dataset;

        public Add(
                int[] _key_dim,
                int _value_dim,
                int _group_cnt,
                float _value_scale,
                String _dataset) {
            key_dim = _key_dim;
            value_dim = _value_dim;
            group_cnt = _group_cnt;
            value_scale = _value_scale;
            dataset = _dataset;
        }

        public ImpHydraStruct call(ImpHydraStruct sketch, String row) throws Exception {
            return AddMany(sketch, row);
        }

        public ImpHydraStruct AddOne(ImpHydraStruct sketch, String row) throws Exception {
            String[] data = utils.mySplit(row, ',');
            String key = data[0];
            String value = data[value_dim];
            sketch.updateOne(key, value);
            return sketch;
        }

        public ImpHydraStruct AddMany(ImpHydraStruct sketch, String row) throws Exception {
            String[] data = utils.mySplit(row, ',');
            String key = data[1]; // dstip
            String value = data[0] + '|' + data[3] + '|' + data[4] + '|' + data[5];
            sketch.updateOne(key, value);
            return sketch;
        }
    }

    static class Combine implements Function2<ImpHydraStruct, ImpHydraStruct, ImpHydraStruct> {
        public ImpHydraStruct call(ImpHydraStruct sketch1, ImpHydraStruct sketch2)
                throws Exception {
            if (sketch1.spark_init) return sketch2;
            if (sketch2.spark_init) return sketch1;
            ImpHydraStruct new_sketch;
            if (sketch1.total_HH_size > sketch2.total_HH_size) {
                new_sketch = sketch1;
                new_sketch.merge(sketch2);
            } else {
                new_sketch = sketch2;
                new_sketch.merge(sketch1);
            }
            return new_sketch;
        }
    }
}
