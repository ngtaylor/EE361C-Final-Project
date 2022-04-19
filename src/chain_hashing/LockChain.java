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

    //True if hash table is empty, false otherwise
    public boolean isEmpty() { return size.get() == 0; }

    //Returns value for a key, returns null if key not found in chain
    public Integer get(Integer key){
        //Use hash function to get index of key
        Integer bucketIdx = hashFunction(key);
        Integer hashCode = key.hashCode();

        //Start at head of chain
        HashNode head = buckets.get(bucketIdx);

        //Find key in its chain
        while(head.key!=null){
            //If found return value, else keep traversing chain
            if(head.key.equals(key) && hashCode.equals(head.hashCode)){
                return head.value;
            } else {
                head = head.next;
            }
        }

        //If not found
        return null;
    }

    //Removes key and returns value associated with it, returns null if not found
    public Integer remove(Integer key){
        //Use hash function to get index of key
        Integer bucketIdx = hashFunction(key);
        Integer hashCode = key.hashCode();

        //Start at head of chain
        HashNode head = buckets.get(bucketIdx);
        head.lock.lock();
        HashNode prev = head;
        HashNode curr = null;
        try {
            if(head.key == null){
                //If head is null then return null
                return null;
            } else if (head.next == null){
                //If non-null head is the only node in chain, and it has same key, remove head
                if (head.key.equals(key) && hashCode.equals(head.hashCode)) {
                    HashNode nullHead = new HashNode(null, null, null);
                    buckets.set(bucketIdx, nullHead);
                    size.decrementAndGet();
                    return head.value;
                } else {
                    return null;
                }
            } else {
                //Else use fine-grained locking to traverse chain
                curr = prev.next;
                curr.lock.lock();
                try {
                    //Find key in its chain
                    while (true) {
                        //If found break, else keep traversing chain
                        if (curr.key.equals(key) && hashCode.equals(curr.hashCode)) {
                            break;
                        } else {
                            if(curr.next == null){
                                //Key not found
                                return null;
                            }
                            //Traverse using "hand over hand" method of acquiring locks
                            prev.lock.unlock();
                            prev = curr;
                            curr = curr.next;
                            curr.lock.lock();
                        }
                    }

                    //Reduce size;
                    size.decrementAndGet();

                    //Remove key
                    if (prev.key != null) {
                        prev.next = curr.next;
                    } else {
                        buckets.set(bucketIdx, curr.next);
                    }
                    return curr.value;
                } finally {
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
            if(head.key == null){
                //If head is null then add new node as head
                size.incrementAndGet();
                HashNode node = new HashNode(key, value, hashCode);
                buckets.set(bucketIdx, node);
            } else if (head.next == null){
                //If non-null head is the only node in chain, and it has same key, update value of head
                if (head.key.equals(key) && hashCode.equals(head.hashCode)) {
                    head.value = value;
                }
            } else {
                //Else use fine-grained locking to traverse chain
                HashNode curr = prev.next;
                curr.lock.lock();
                try {
                    //See if key is already in its chain
                    while (true) {
                        //If found return value, else keep traversing chain
                        if (curr.key.equals(key) && hashCode.equals(curr.hashCode)) {
                            curr.value = value;
                            break;
                        } else {
                            if(curr.next == null){
                                break;
                            }
                            //Traverse using "hand over hand" method of acquiring locks
                            prev.lock.unlock();
                            prev = curr;
                            curr = curr.next;
                            curr.lock.lock();
                        }
                    }

                    //If key not present then insert it into chain between previous and current nodes to ensure it gets added safely
                    size.incrementAndGet();
                    HashNode node = new HashNode(key, value, hashCode);
                    node.next = curr;
                    prev.next = node;
                } finally {
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