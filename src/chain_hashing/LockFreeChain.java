/* LockFreeChain.java
 * EE361C Final Project
 */

package chain_hashing;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LockFreeChain {
    //Stores array of chains
    private ArrayList<AtomicReference<HashNode>> buckets;

    //Current capacity of array list
    private Integer numBuckets;

    //Current size of array list
    public AtomicInteger size;

    public LockFreeChain(){
        buckets = new ArrayList<>();
        numBuckets = 10;
        size = new AtomicInteger();

        //Make empty chains
        for(int i = 0; i < numBuckets; i++){
            HashNode node = new HashNode(null, null, null);
            buckets.add(new AtomicReference<>(node));
        }
    }

    public LockFreeChain(Integer numBuckets){
        this.numBuckets = numBuckets;
        buckets = new ArrayList<>();
        size = new AtomicInteger();

        //Make empty chains
        for(int i = 0; i < numBuckets; i++){
            HashNode node = new HashNode(null, null, null);
            buckets.add(new AtomicReference<>(node));
        }
    }

    //Gets buckets array list for testing purposes
    public ArrayList<AtomicReference<HashNode>> getBuckets(){
        return buckets;
    }

    //True if hash table is empty, false otherwise
    public boolean isEmpty() { return size.get() == 0; }

    //Returns value for a key
    public Integer get(Integer key){
        //Use hash function to get index of key
        Integer bucketIdx = hashFunction(key);
        Integer hashCode = key.hashCode();

        //Start at head of chain
        AtomicReference<HashNode> head = buckets.get(bucketIdx);

        //Find key in its chain
        while(head.get().key != null){
            //If found return value, else keep traversing chain
            if(head.get().key.equals(key) && hashCode.equals(head.get().hashCode)){
                return head.get().value;
            } else {
                head = head.get().next;
            }
        }

        //If not found
        return null;
    }

    //Removes key and returns value associated with it
    public Integer remove(Integer key){
        //Use hash function to get index of key
        Integer bucketIdx = hashFunction(key);
        Integer hashCode = key.hashCode();

        //Start at head of chain
        AtomicReference<HashNode> head = buckets.get(bucketIdx);

        //Find key in its chain
        AtomicReference<HashNode> prev = null;
        while(head.get().key != null){
            //If found break, else keep traversing chain
            if(head.get().key.equals(key) && hashCode.equals(head.get().hashCode)){
                break;
            } else {
                prev = head;
                head = head.get().next;
            }
        }

        //If key not found
        if(head.get().key == null){
            return null;
        }

        //Reduce size;
        size.decrementAndGet();

        //Remove key
        if(prev != null && prev.get().key != null){
            HashNode prevNode = prev.get();
            prevNode.next = head.get().next;
            prev.set(prevNode);
        } else {
            buckets.set(bucketIdx, head.get().next);
        }
        return head.get().value;
    }

    //Add key-value pair to hash table
    public void put(Integer key, Integer value){
        //Use hash function to get index of key
        Integer bucketIdx = hashFunction(key);
        Integer hashCode = key.hashCode();

        //Start at head of chain
        AtomicReference<HashNode> head = buckets.get(bucketIdx);

        //See if key is already in its chain
        while(head.get().key != null){
            //If found break, else keep traversing chain
            if(head.get().key.equals(key) && hashCode.equals(head.get().hashCode)){
                HashNode newNode = head.get();
                newNode.value = value;
                HashNode oldHead = head.get();
                head.compareAndSet(oldHead, newNode);
                return;
            } else {
                head = head.get().next;
            }
        }

        //If key not present then insert it as new head
        size.incrementAndGet();
        head = buckets.get(bucketIdx);
        HashNode node = new HashNode(key, value, hashCode);
        node.next = head;
        AtomicReference<HashNode> newHead = new AtomicReference<>(node);
        buckets.set(bucketIdx, newHead);
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