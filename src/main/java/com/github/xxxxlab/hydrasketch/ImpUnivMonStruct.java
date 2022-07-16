/* (C)2021 */
package com.github.xxxxlab.hydrasketch;

import java.io.*;
import java.util.*;
import net.jpountz.xxhash.*;

public class ImpUnivMonStruct {
    public ImpCountSketchStruct[] um;
    public int k; // top K
    public int layer; // layer
    public int numRows; // row
    public int countersPerRow; // col
    public int totalCount = 0;
    public static XXHash64 hashFunc;
    public int um_seed;
    public float est_l2 = 0;
    public float est_entropy = 0;
    public float est_l1 = 0;
    public float est_card = 0;

    private static final int UPDATE_HEAP_RATE = 1;
    private static final int UPDATE_HEAP_LAYER = 1;

    public ImpUnivMonStruct(int layer, int numRows, int countersPerRow, int k, int um_seed) {
        this.k = k;
        this.layer = layer;
        this.numRows = numRows;
        this.countersPerRow = countersPerRow;
        this.hashFunc = XXHashFactory.fastestInstance().hash64();
        this.um_seed = um_seed;

        this.um = new ImpCountSketchStruct[layer];
        for (int i = 0; i < layer; i++) {
            um[i] = new ImpCountSketchStruct(k, numRows, countersPerRow);
        }
    }

    public static int findBottomLayerNum(int hash, int layer) {
        // optimization: hash only once
        // if hash mod 2 = 1, then go down
        for (int l = 0; l < layer - 1; l++) {
            if (((hash >> l) & 1) == 0) return l;
        }
        return layer - 1;
    }

    public void update(String insert, int[] pos, int[] sign, int bottom_layer_num) {
        totalCount++;
        int median = um[bottom_layer_num].add(pos, sign);
        um[bottom_layer_num].updateHeap(insert, median, 1);
    }

    public void printTopHeap() {
        for (ImpPair hh : um[0].getTopK()) {
            System.out.printf("top heap, %s %d\n", hh.key, hh.value);
        }
    }

    public float calcGSum(String key, GFunction g) {
        return calcGSum_Heuristic(key, g);
    }

    private void processHHforHeuristic() {
        for (int l1 = 0; l1 <= layer - 1; l1++) {
            for (int i = 0; i < um[l1].topK.size; i++) {
                ImpPair hh = um[l1].topK.Heap[i];
                for (int l2 = l1 - 1; l2 >= 0; l2--) {
                    um[l2].topK.update(hh.key, hh.value, 0);
                }
            }
        }

        for (int l1 = layer - 1; l1 >= 0; l1--) {
            um[l1].HH_table = new HashMap<String, ArrayList<ImpPair>>();

            for (int i = 0; i < um[l1].topK.size; i++) {
                ImpPair hh = um[l1].topK.Heap[i];
                // try to find if this hh belongs to upper layer
                boolean find = false;
                for (int l2 = l1 - 1; l2 >= 0; l2--) {
                    if (um[l2].topK.contain(hh.key)) {
                        find = true;
                        break;
                    }
                }
                if (find) continue;

                // this hh belongs to this layer
                String[] keys = hh.key.split(":", -1);
                String group_key = keys[0]; // group
                um[l1].HH_table.putIfAbsent(group_key, new ArrayList<ImpPair>());
                um[l1].HH_table.get(group_key).add(hh);
            }
            um[l1].topK = null;
        }
    }

    // when query, find whether HH belongs to upper layer
    // separate HH in heap by interesting group
    private void processForHashset() {
        for (int l1 = layer - 1; l1 >= 0; l1--) {
            um[l1].HH_table = new HashMap<String, ArrayList<ImpPair>>();

            for (int i = 0; i < um[l1].topK.size; i++) {
                ImpPair hh = um[l1].topK.Heap[i];
                // try to find if this hh belongs to upper layer
                boolean find = false;
                for (int l2 = l1 - 1; l2 >= 0; l2--) {
                    if (um[l2].topK.contain(hh.key)) {
                        find = true;
                        break;
                    }
                }
                if (find) continue;

                // this hh belongs to this layer
                String[] keys = hh.key.split(":", -1);
                // String group_key = "dd"; // gsum add everything in the UM
                String group_key = keys[0]; // group
                // String hh_key = keys[1]; // metric value
                um[l1].HH_table.putIfAbsent(group_key, new ArrayList<ImpPair>());
                um[l1].HH_table.get(group_key).add(hh);
            }
            um[l1].topK = null;
        }
    }

    public float calcGSum_Heuristic(String key, GFunction g) {
        if (um[0].HH_table == null) {
            processHHforHeuristic();
        }
        float sum = 0;
        for (int l = 0; l < layer; l++) {
            float tmp = 0;
            ArrayList<ImpPair> HH_list = um[l].HH_table.get(key);
            if (HH_list == null) continue;
            for (ImpPair hh : HH_list) {
                tmp += g.calc(hh.value);
            }
            sum += tmp * (1 << l);
        }
        return sum;
    }

    public float calcL2(String key) {
        return calcGSum(key, x -> x * x);
    }

    public float calcL1(String key) {
        return calcGSum(key, x -> x);
    }

    public float calcEntropy(String key) {
        return calcGSum(key, x -> x * utils.log2(x));
    }

    public float calcCard(String key) {
        return calcGSum(key, x -> 1);
    }

    public float calcL2Once(String key) {
        if (est_l2 == 0) est_l2 = calcGSum(key, x -> x * x);
        return est_l2;
    }

    public float calcL1Once(String key) {
        if (est_l1 == 0) est_l1 = calcGSum(key, x -> x);
        return est_l1;
    }

    public float calcEntropyOnce(String key) {
        if (est_entropy == 0) est_entropy = calcGSum(key, x -> x * utils.log2(x));
        return est_entropy;
    }

    public float calcCardOnce(String key) {
        if (est_card == 0) est_card = calcGSum(key, x -> 1);
        return est_card;
    }

    public void mergeNaive(ImpUnivMonStruct other) {
        final Boolean Merge_Sketch = false;
        if (Merge_Sketch) {
            for (int l_indx = 0; l_indx < this.layer; l_indx++) {
                ImpCountSketchStruct this_cs = this.um[l_indx];
                ImpCountSketchStruct other_cs = other.um[l_indx];
                if (other_cs.cs == null) continue;
                if (this_cs.cs == null) this_cs.cs = new int[this.numRows * this.countersPerRow];
                for (int i = 0; i < this.numRows * this.countersPerRow; i++) {
                    this_cs.cs[i] += other_cs.cs[i];
                }
            }
        }

        // merge totalCount
        this.totalCount += other.totalCount;

        // merge HH heap
        for (int l_indx = 0; l_indx < this.layer; l_indx++) {
            ImpCountSketchStruct this_cs = this.um[l_indx];
            ImpCountSketchStruct other_cs = other.um[l_indx];

            for (int i = 0; i < other_cs.topK.size; i++) {
                ImpPair hh = other_cs.topK.Heap[i];
                this_cs.updateHeap(hh.key, hh.value, hh.value);
            }
        }
    }
}
