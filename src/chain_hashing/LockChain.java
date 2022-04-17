/* LockChain.java
 * EE361C Final Project
 */

package chain_hashing;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class LockChain {
    private ReentrantLock lock;

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
        lock = new ReentrantLock();
    }

    public LockChain(Integer numBuckets){
        this.numBuckets = numBuckets;
        buckets = new ArrayList<>();
        size = 0;
        lock = new ReentrantLock();
    }

    //True if hash table is empty, false otherwise
    public boolean isEmpty() { return size == 0; }

    //Returns value for a key
    public Integer get(Integer key){

        return null;
    }

    //Removes key and returns value associated with it
    public Integer remove(Integer key){

        return null;
    }

    //Add key-value pair to hash table
    public void put(Integer key, Integer value){

    }

    //Hash function for obtaining hash index for a key
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