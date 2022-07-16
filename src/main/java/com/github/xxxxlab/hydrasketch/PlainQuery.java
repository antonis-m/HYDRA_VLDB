/* (C)2021 */
package com.github.xxxxlab.hydrasketch;

import java.io.*;
import java.util.*;

public class PlainQuery {
    public static void main(String[] args) {
        String expFile = args[0];
        String debug_on = "0";
        String prefix = "1";
        HashMap<String, String> expMap = utils.ReadExp(expFile);
        utils.PrintExp(expMap);
        // String input_file = expMap.get("input_file");
        String config_file = expMap.get("config_file");
        String res_file = expMap.get("res_file");
        String save_plain_file = expMap.get("save_plain_file");
        String sketch_file = expMap.get("sketch_file");
        HashMap<String, Integer> configMap = utils.ReadConfig(config_file);

        int[] key_dim = utils.ParseKeyDim(expMap.get("key_dim"));
        int key_dim_len = key_dim.length;
        int value_dim = Integer.parseInt(expMap.get("value_dim"));
        float value_scale = Float.parseFloat(expMap.get("value_scale"));

        ImpHydraStruct res_sketch = DeserializeSketch(sketch_file);

        try {
            File f = new File(save_plain_file);
            f.delete();
        } catch (Exception e) {
            System.out.println(e);
        }

        // query
        MyTimer query_timer = new MyTimer("query");
        oldQueryBatch(res_sketch, res_file, save_plain_file, key_dim_len);
        query_timer.Stop();

        res_sketch.executor.shutdown();
    }

    static ImpHydraStruct DeserializeSketch(String sketch_file) {
        try {
            MyTimer deserialize_timer = new MyTimer("deserialize");
            FileInputStream fileIn = new FileInputStream(sketch_file);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            ImpHydraStruct sketch = (ImpHydraStruct) in.readObject();
            in.close();
            fileIn.close();
            deserialize_timer.Stop();
            return sketch;
        } catch (Exception i) {
            System.out.printf("Deserialize fails %s\n", i);
            i.printStackTrace();
        }
        return null;
    }

    static void oldQuery(
            ImpHydraStruct sketch, String res_file, String save_file, int key_dim_len) {
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader(res_file));
            PrintWriter printWriter = new PrintWriter(new FileWriter(save_file));
            String row;
            int line = 0;
            while ((row = csvReader.readLine()) != null) {
                line++;
                String[] data = utils.mySplit(row, ',');
                // String key = utils.makeKey(data, key_dim_len);
                String key = data[0];

                float[] estimate = sketch.queryOneNoCheck(key);
                printWriter.printf(
                        "%s,%.2f,%.2f,%.2f,%.2f\n",
                        key, estimate[0], estimate[1], estimate[2], estimate[3]);
            }
            csvReader.close();
            printWriter.close();
            System.out.printf("query size %d\n", line);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    static void oldQueryBatch(
            ImpHydraStruct sketch, String res_file, String save_file, int key_dim_len) {
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader(res_file));
            PrintWriter printWriter = new PrintWriter(new FileWriter(save_file));
            String row;
            int line = 0;
            ArrayList<String> keys = new ArrayList<String>();
            final int BATCH_SIZE = 100000;
            while ((row = csvReader.readLine()) != null) {
                line++;
                String[] data = utils.mySplit(row, ',');
                // String key = utils.makeKey(data, key_dim_len);
                String key = data[0];
                Float group_size = Float.parseFloat(data[4]);
                // if (group_size < 1e4) continue;
                keys.add(key);

                if (keys.size() >= BATCH_SIZE) {
                    System.out.printf("line %d\n", line);
                    callBatchQuery(sketch, keys, printWriter);
                    keys.clear();
                }
            }
            callBatchQuery(sketch, keys, printWriter);

            csvReader.close();
            printWriter.close();
            System.out.printf("query size %d\n", line);
        } catch (Exception e) {
            e.printStackTrace(System.out);
            System.out.println(e);
        }
    }

    static void callBatchQuery(
            ImpHydraStruct sketch, ArrayList<String> keys, PrintWriter printWriter) {
        ArrayList<QueryResult> query_results = sketch.queryBatchNoCheck(keys);
        for (int i = 0; i < keys.size(); i++) {
            QueryResult qr = query_results.get(i);
            printWriter.printf(
                    "%s, %.2f, %.2f, %.2f, %.2f\n", keys.get(i), qr.entropy, qr.l2, qr.l1, qr.card);
        }
    }
}
