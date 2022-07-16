/* (C)2021 */
package com.github.xxxxlab.hydrasketch;

import java.util.*;
import java.util.concurrent.Callable;
import net.jpountz.xxhash.*;

class UpdateTask implements Callable<Integer> {
    public ImpUnivMonStruct[] hydraSketchRow;
    public int hs_seed;
    public int d_indx;
    public int w;
    public int numRows;
    public int countersPerRow;
    public int levels;
    public XXHash64 hashFunc;
    public ArrayList<StringPair> datas;

    public UpdateTask(
            ImpUnivMonStruct[] hydraSketchRow,
            int hs_seed,
            int d_indx,
            int w,
            int numRows,
            int countersPerRow,
            int levels,
            ArrayList<StringPair> datas) {
        this.hydraSketchRow = hydraSketchRow;
        this.hs_seed = hs_seed;
        this.d_indx = d_indx;
        this.w = w;
        this.numRows = numRows;
        this.countersPerRow = countersPerRow;
        this.levels = levels;
        this.hashFunc = XXHashFactory.fastestInstance().hash64();
        this.datas = datas;
    }

    @Override
    public Integer call() throws Exception {
        // init UM seed
        Random umGenerator = utils.setUMSeed();
        int um_seed = utils.getSeedRandom(umGenerator);

        for (StringPair kv : datas) {
            String key = kv.key;
            String value = kv.value;

            // calculate CS position
            String insert = key + ":" + value;
            byte[] byte_kv = utils.StringToByte(insert);
            long hash_kv_long = hashFunc.hash(byte_kv, 0, byte_kv.length, um_seed);
            int[] hash_kv = utils.getHashArray(hash_kv_long);
            int[] pos = new int[numRows];
            int[] sign = new int[numRows];
            for (int r = 0; r < numRows; r++) {
                pos[r] = hash_kv[r] % countersPerRow;
                sign[r] = 2 * (hash_kv[r] & 1) - 1;
            }
            // calculate bottom layer
            int bottom_layer_num = ImpUnivMonStruct.findBottomLayerNum(hash_kv[3], levels);

            byte[] byte_key = utils.StringToByte(key);
            long hash_key_long = hashFunc.hash(byte_key, 0, byte_key.length, hs_seed);
            int[] hash_key = utils.getHashArray(hash_key_long);
            int hash = hash_key[d_indx] % w;
            hydraSketchRow[hash].update(insert, pos, sign, bottom_layer_num);
        }
        return 1; // 1 for success
    }
}

class QueryTask implements Callable<ArrayList<QueryResult>> {
    public ImpUnivMonStruct[] hydraSketchRow;
    public ArrayList<String> keys;
    public int[] hashs;

    public QueryTask(ImpUnivMonStruct[] hydraSketchRow, ArrayList<String> keys, int[] hashs) {
        this.hydraSketchRow = hydraSketchRow;
        this.keys = keys;
        this.hashs = hashs;
    }

    @Override
    public ArrayList<QueryResult> call() throws Exception {
        ArrayList<QueryResult> results = new ArrayList<QueryResult>();
        int keys_len = keys.size();
        for (int i = 0; i < keys_len; i++) {
            String key = keys.get(i);
            ImpUnivMonStruct umSketch = hydraSketchRow[hashs[i]];
            float entropy = umSketch.calcEntropy(key);
            float l2 = umSketch.calcL2(key);
            float l1 = umSketch.calcL1(key);
            float card = umSketch.calcCard(key);
            results.add(new QueryResult(entropy, l2, l1, card));
        }
        return results;
    }
}

/*
class QueryMergeTask implements Callable<ArrayList<QueryResult>> {
    public ImpUnivMonStruct univSketch;
    public String keys;

    public QueryMergeTask(ImpUnivMonStruct univSketch, String keys) {
        this.univSketch = univSketch;
        this.keys = keys;
    }

    @Override
    public ArrayList<QueryResult> call() throws Exception {
        ArrayList<QueryResult> results = new ArrayList<QueryResult>();

        byte[] data = utils.StringToByte(this.keys);
        ImpUnivMonStruct umSketch = this.univSketch;
        float entropy = umSketch.calcEntropy(this.keys);
        float l2 = umSketch.calcL2(this.keys);
        float card = umSketch.calcCard(this.keys);
        results.add(new QueryResult(entropy, l2, l1, card));

        return results;
    }
} */
