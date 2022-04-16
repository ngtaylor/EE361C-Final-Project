package chain_hashing;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

public class LockFreeChain {
    //Stores array of chains
    private ArrayList<AtomicReference<HashNode>> buckets;

    //Current capacity of array list
    private Integer numBuckets;

    //Current size of array list
    private Integer size;

    public LockFreeChain(){
        buckets = new ArrayList<>();
        numBuckets = 10;
        size = 0;
    }

    public LockFreeChain(Integer numBuckets){
        this.numBuckets = numBuckets;
        buckets = new ArrayList<>();
        size = 0;
    }

    public int size() { return size; }
    public boolean isEmpty() { return size() == 0; }

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
        public AtomicReference<HashNode> next;
        final Integer hashCode;

        public HashNode(Integer key, Integer value, Integer hashCode){
            this.key = key;
            this.value = value;
            this.hashCode = hashCode;
            next = new AtomicReference<>(null);
        }
    }
}