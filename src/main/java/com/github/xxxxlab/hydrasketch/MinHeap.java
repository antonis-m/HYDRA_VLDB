/* (C)2021 */
package com.github.xxxxlab.hydrasketch;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

public class MinHeap {
    public ImpPair[] Heap;
    public int size;
    public int maxsize;
    private int ksize;
    public Object2IntOpenHashMap<String> key_index; // map a key to index in heap

    public MinHeap(int maxsize) {
        this.maxsize = maxsize;
        this.ksize = maxsize;
        this.size = 0;
    }

    public void allocateHeap(int heap_size) {
        Heap = new ImpPair[heap_size];
    }

    // Returns position of parent
    private int parent(int pos) {
        return (pos - 1) / 2;
    }

    // Below two functions return left and
    // right children.
    private int leftChild(int pos) {
        return 2 * pos + 1;
    }

    private int rightChild(int pos) {
        return 2 * pos + 2;
    }

    // Returns true of given node is leaf
    private boolean isLeaf(int pos) {
        return pos < size && leftChild(pos) >= size;
    }

    private void swap(int fpos, int spos) {
        ImpPair tmp;
        tmp = Heap[fpos];
        Heap[fpos] = Heap[spos];
        Heap[spos] = tmp;

        int tmp_i = key_index.get(Heap[fpos].key);
        key_index.put(Heap[fpos].key, key_index.get(Heap[spos].key));
        key_index.put(Heap[spos].key, tmp_i);
    }

    // A recursive function to max heapify the given
    // subtree. This function assumes that the left and
    // right subtrees are already heapified, we only need
    // to fix the root.
    private void HeapDown(int pos) {
        int smallest = pos;
        if (leftChild(pos) < size && Heap[leftChild(pos)].value < Heap[smallest].value)
            smallest = leftChild(pos);
        if (rightChild(pos) < size && Heap[rightChild(pos)].value < Heap[smallest].value)
            smallest = rightChild(pos);
        if (smallest != pos) {
            swap(pos, smallest);
            HeapDown(smallest);
        }
    }

    // Inserts a new Heapent to min heap
    private void insert(ImpPair p) {
        Heap[size] = p;
        key_index.put(p.key, size);
        int current = size;
        size++;

        while (current > 0 && Heap[current].value < Heap[parent(current)].value) {
            swap(current, parent(current));
            current = parent(current);
        }
    }

    // no need to do heapdown, simply push_back
    public void readback(String item, int count) {
        Heap[size] = new ImpPair(item, count);
        size++;
    }

    private void init_key_index() {
        // key_index = new HashMap<String, Integer>();
        // key_index = new TObjectIntHashMap<String>();
        key_index = new Object2IntOpenHashMap<String>();
        for (int i = 0; i < size; i++) key_index.put(Heap[i].key, i);
    }

    // CountSketch should only call this to update
    // if exist, increase count by add
    // else, set count = count
    // return if item is HH after update
    public boolean update(String item, int count, int add) {
        if (Heap == null) allocateHeap(this.maxsize);
        if (key_index == null) init_key_index();

        if (key_index.containsKey(item)) {
            int index = key_index.get(item);
            Heap[index].value += add;
            HeapDown(index);
            return true;
        }

        // we first allocate a small heap, and resize it if needed
        if (size == Heap.length && size < maxsize) {
            ImpPair[] newHeap = new ImpPair[this.maxsize];
            for (int i = 0; i < Heap.length; i++) newHeap[i] = Heap[i];
            this.Heap = newHeap;
        }

        if (size < maxsize) {
            insert(new ImpPair(item, count));
            return true;
        } else if (count > Heap[0].value) {
            key_index.remove(Heap[0].key);
            key_index.put(item, 0);
            Heap[0].key = item;
            Heap[0].value = count;
            HeapDown(0);
            return true;
        }
        return false;
    }

    public boolean tryInsert(String item, int count) {
        if (key_index == null) init_key_index();

        if (key_index.containsKey(item)) {
            return true;
        }

        // we first allocate a small heap, and resize it if needed
        if (size == Heap.length && size < maxsize) {
            ImpPair[] newHeap = new ImpPair[this.maxsize];
            for (int i = 0; i < Heap.length; i++) newHeap[i] = Heap[i];
            this.Heap = newHeap;
        }

        if (size < ksize) {
            insert(new ImpPair(item, count));
            return true;
        } else if (count > Heap[0].value) {
            key_index.remove(Heap[0].key);
            key_index.put(item, 0);
            Heap[0].key = item;
            Heap[0].value = count;
            HeapDown(0);
            return true;
        } else if (count == Heap[0].value) {
            if (size == maxsize) {
                // we expand heap by k
                resizeHeap();
            }
            insert(new ImpPair(item, count));
            return true;
        }
        return false;
    }

    public boolean contain(String item) {
        if (key_index == null) init_key_index();
        return key_index.containsKey(item);
    }

    public void resizeHeap() {
        this.maxsize += this.ksize;
        ImpPair[] newHeap = new ImpPair[this.maxsize];
        for (int i = 0; i < Heap.length; i++) newHeap[i] = Heap[i];
        this.Heap = newHeap;
    }

    public void clearHeap() {
        this.Heap = new ImpPair[this.maxsize];
        this.size = 0;
    }

    public ImpPair[] getHeap() {
        ImpPair[] res = new ImpPair[size];
        for (int i = 0; i < size; i++) res[i] = Heap[i];
        return res;
    }

    public int getSize() {
        return size;
    }

    public void print() {
        for (int i = 0; i < size; i++) {
            System.out.printf("heap print, %d %s %d\n", i, Heap[i].key, Heap[i].value);
        }
    }
}
