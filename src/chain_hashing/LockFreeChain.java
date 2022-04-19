/* LockFreeChain.java
 * EE361C Final Project
 */

package chain_hashing;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;

public class LockFreeChain {
    //Stores array of chains
    private ArrayList<AtomicMarkableReference<HashNode>> buckets;

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
            buckets.add(new AtomicMarkableReference<>(node, false));
        }
    }

    public LockFreeChain(Integer numBuckets){
        this.numBuckets = numBuckets;
        buckets = new ArrayList<>();
        size = new AtomicInteger();

        //Make empty chains
        for(int i = 0; i < numBuckets; i++){
            HashNode node = new HashNode(null, null, null);
            buckets.add(new AtomicMarkableReference<>(node, false));
        }
    }

    //Gets buckets array list for testing purposes
    public ArrayList<AtomicMarkableReference<HashNode>> getBuckets(){
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
        AtomicMarkableReference<HashNode> head = buckets.get(bucketIdx);

        //Find key in its chain
        while(head.getReference().key != null){
            //If found return value, else keep traversing chain
            if(head.getReference().key.equals(key) && hashCode.equals(head.getReference().hashCode)){
                return head.getReference().value;
            } else {
                head = head.getReference().next;
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
        AtomicMarkableReference<HashNode> head = buckets.get(bucketIdx);

        //Find key in its chain
        AtomicMarkableReference<HashNode> prev = null;
        while(head.getReference().key != null){
            //If found break, else keep traversing chain
            if(head.getReference().key.equals(key) && hashCode.equals(head.getReference().hashCode)){
                break;
            } else {
                prev = head;
                head = head.getReference().next;
            }
        }

        //If key not found
        if(head.getReference().key == null){
            return null;
        }

        //Reduce size;
        size.decrementAndGet();

        //Remove key
        if(prev != null && prev.getReference().key != null){
            HashNode prevNode = prev.getReference();
            prevNode.next = head.getReference().next;
            prev.set(prevNode, false);
        } else {
            buckets.set(bucketIdx, head.getReference().next);
        }
        return head.getReference().value;
    }

    //Add key-value pair to hash table
    public void put(Integer key, Integer value){
        //Use hash function to get index of key
        Integer bucketIdx = hashFunction(key);
        Integer hashCode = key.hashCode();

        //Start at head of chain
        AtomicMarkableReference<HashNode> head = buckets.get(bucketIdx);

        //See if key is already in its chain
        while(head.getReference().key != null){
            //If found break, else keep traversing chain
            if(head.getReference().key.equals(key) && hashCode.equals(head.getReference().hashCode)){
                HashNode newNode = head.getReference();
                newNode.value = value;
                HashNode oldHead = head.getReference();
                head.compareAndSet(oldHead, newNode, false, false);
                return;
            } else {
                head = head.getReference().next;
            }
        }

        //If key not present then insert it as new head
        size.incrementAndGet();
        head = buckets.get(bucketIdx);
        HashNode node = new HashNode(key, value, hashCode);
        node.next = head;
        AtomicMarkableReference<HashNode> newHead = new AtomicMarkableReference<>(node, false);
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
        public AtomicMarkableReference<HashNode> next;
        final Integer hashCode;

        public HashNode(Integer key, Integer value, Integer hashCode){
            this.key = key;
            this.value = value;
            this.hashCode = hashCode;
            next = new AtomicMarkableReference<>(null, false);
        }
    }
}