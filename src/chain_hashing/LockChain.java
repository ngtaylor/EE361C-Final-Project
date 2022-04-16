package chain_hashing;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class LockChain {
    //Stores array of chains
    private ArrayList<HashNode> buckets;

    //Current capacity of array list
    private Integer numBuckets;

    //Current size of array list
    private Integer size;

    public LockChain(){
        buckets = new ArrayList<>();
        numBuckets = 10;
        size = 0;
    }

    public LockChain(Integer numBuckets){
        this.numBuckets = numBuckets;
        buckets = new ArrayList<>();
        size = 0;
    }

    public int size() { return size; }
    public boolean isEmpty() { return size() == 0; }

    private final Integer hashCode(Integer key) {
        return Integer.hashCode(key);
    }

    public Integer get(Integer key){

        return null;
    }

    public Integer remove(Integer key){

        return null;
    }

    public void put(Integer key, Integer value){

    }

    private Integer hashFunction(Integer key){
        return null;
    }

    protected class HashNode {
        public Integer key;
        public Integer value;
        public HashNode next;
        final Integer hashCode;

        public HashNode(Integer key, Integer value, Integer hashCode){
            this.key = key;
            this.value = value;
            this.hashCode = hashCode;
        }
    }
}