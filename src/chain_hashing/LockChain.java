/* LockChain.java
 * EE361C Final Project
 */

package chain_hashing;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class LockChain {
    //Stores array of chains
    private ArrayList<HashNode> buckets;

    //Current capacity of array list
    private Integer numBuckets;

    //Current size of array list
    public AtomicInteger size;

    public LockChain(){
        buckets = new ArrayList<>();
        numBuckets = 10;
        size = new AtomicInteger();

        //Make empty chains
        for(int i = 0; i < numBuckets; i++){
            buckets.add(new HashNode(null, null, null));
        }
    }

    public LockChain(Integer numBuckets){
        this.numBuckets = numBuckets;
        buckets = new ArrayList<>();
        size = new AtomicInteger();

        //Make empty chains
        for(int i = 0; i < numBuckets; i++){
            buckets.add(new HashNode(null, null, null));
        }
    }

    //Gets buckets array list for testing purposes
    public ArrayList<HashNode> getBuckets(){
        return buckets;
    }

    //True if hash table is empty, false otherwise
    public boolean isEmpty() { return size.get() == 0; }

    //Returns value for a key, returns null if key not found in chain
    public Integer get(Integer key){
        //Use hash function to get index of key
        Integer bucketIdx = hashFunction(key);
        Integer hashCode = key.hashCode();

        //Start at head of chain
        HashNode head = buckets.get(bucketIdx);
        HashNode prev = null;
        HashNode curr = null;
        head.lock.lock();
        try {
            prev = head;
            curr = prev.next;
            if(curr == null){
                return null;
            }
            curr.lock.lock();
            try {
                //Find key in its chain
                while (curr.hashCode < hashCode) {
                    //Traverse using "hand over hand" method of acquiring locks
                    prev.lock.unlock();
                    prev = curr;
                    curr = curr.next;
                    if(curr == null) {
                        break;
                    }
                    curr.lock.lock();
                }

                if(curr != null && curr.hashCode == hashCode){
                    return curr.value;
                }

                //If not found
                return null;
            } finally {
                if(curr != null) {
                    curr.lock.unlock();
                }
            }
        } finally {
            prev.lock.unlock();
        }
    }

    //Removes key and returns value associated with it, returns null if not found
    public Integer remove(Integer key){
        //Use hash function to get index of key
        Integer bucketIdx = hashFunction(key);
        Integer hashCode = key.hashCode();

        //Start at head of chain
        HashNode head = buckets.get(bucketIdx);
        HashNode prev = null;
        HashNode curr = null;
        head.lock.lock();
        try {
            //Else use fine-grained locking to traverse chain
            prev = head;
            curr = prev.next;
            if(curr == null){
                return null;
            }
            curr.lock.lock();
            try {
                //Find key in its chain
                while (curr.hashCode < hashCode) {
                    //Traverse using "hand over hand" method of acquiring locks
                    prev.lock.unlock();
                    prev = curr;
                    curr = curr.next;
                    if(curr == null) {
                        break;
                    }
                    curr.lock.lock();
                }

                if(curr != null && curr.hashCode == hashCode){
                    //Reduce size;
                    size.decrementAndGet();
                    prev.next = curr.next;
                    return curr.value;
                }
                return null;
            } finally {
                if(curr != null) {
                    curr.lock.unlock();
                }
            }
        } finally {
            prev.lock.unlock();
        }
    }

    //Add key-value pair to hash table
    public void put(Integer key, Integer value){
        //Use hash function to get index of key
        Integer bucketIdx = hashFunction(key);
        Integer hashCode = key.hashCode();

        //Start at head of chain
        HashNode head = buckets.get(bucketIdx);
        head.lock.lock();
        HashNode prev = head;
        try {
            //Else use fine-grained locking to traverse chain
            HashNode curr = prev.next;
            if(curr == null){
                size.incrementAndGet();
                HashNode node = new HashNode(key, value, hashCode);
                node.next = null;
                prev.next = node;
                return;
            }
            curr.lock.lock();
            try {
                //See if key is already in its chain
                while (curr.hashCode < hashCode) {
                        //Traverse using "hand over hand" method of acquiring locks
                        prev.lock.unlock();
                        prev = curr;
                        curr = curr.next;
                        if(curr == null) {
                            break;
                        }
                        curr.lock.lock();
                }
                if(curr != null && curr.hashCode == hashCode){
                    curr.value = value;
                    return;
                }

                //If key not present then insert it after current to ensure it is added safely
                size.incrementAndGet();
                HashNode node = new HashNode(key, value, hashCode);
                node.next = curr;
                prev.next = node;
            } finally {
                if(curr != null) {
                    curr.lock.unlock();
                }
            }
        } finally {
            prev.lock.unlock();
        }
    }

    //Hash function for obtaining hash index for a key
    private Integer hashFunction(Integer key){
        Integer hashCode = key.hashCode();
        Integer index = hashCode % numBuckets;
        index = index < 0 ? index * -1 : index;
        return index;
    }

    protected class HashNode {
        public Integer key;
        public Integer value;
        public HashNode next;
        final Integer hashCode;

        //Fine grain locking requires each node to have its own lock
        public ReentrantLock lock;

        public HashNode(Integer key, Integer value, Integer hashCode){
            this.key = key;
            this.value = value;
            this.hashCode = hashCode;
            next = null;
            lock = new ReentrantLock();
        }
    }
}