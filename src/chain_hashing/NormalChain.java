/* NormalChain.java
 * EE361C Final Project
 */

package chain_hashing;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicMarkableReference;

public class NormalChain {
    //Stores array of chains
    private ArrayList<HashNode> buckets;

    //Current capacity of array list
    private Integer numBuckets;

    //Current size of array list
    public Integer size;

    public NormalChain(){
        buckets = new ArrayList<>();
        numBuckets = 10;
        size = 0;

        //Make empty chains
        for(int i = 0; i < numBuckets; i++){
            buckets.add(new HashNode(null, null, null));
        }
    }

    public NormalChain(Integer numBuckets){
        this.numBuckets = numBuckets;
        buckets = new ArrayList<>();
        size = 0;

        //Make empty chains
        for(int i = 0; i < numBuckets; i++){
            buckets.add(new HashNode(null, null, null));
        }
    }

    //True if hash table is empty, false otherwise
    public boolean isEmpty() { return size == 0; }

    //Returns value for a key
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

    //Removes key and returns value associated with it
    public Integer remove(Integer key){
        //Use hash function to get index of key
        Integer bucketIdx = hashFunction(key);
        Integer hashCode = key.hashCode();

        //Start at head of chain
        HashNode head = buckets.get(bucketIdx);

        //Find key in its chain
        HashNode prev = null;
        while(head.key!=null){
            //If found break, else keep traversing chain
            if(head.key.equals(key) && hashCode.equals(head.hashCode)){
                break;
            } else {
                prev = head;
                head = head.next;
            }
        }

        //If key not found
        if(head.key == null){
            return null;
        }

        //Reduce size;
        size--;

        //Remove key
        if(prev != null){
            prev.next = head.next;
        } else {
            buckets.set(bucketIdx, head.next);
        }
        return head.value;
    }

    //Add key-value pair to hash table
    public void put(Integer key, Integer value){
        //Use hash function to get index of key
        Integer bucketIdx = hashFunction(key);
        Integer hashCode = key.hashCode();

        //Start at head of chain
        HashNode head = buckets.get(bucketIdx);

        //See if key is already in its chain
        while(head.key!=null){
            //If found return value, else keep traversing chain
            if(head.key.equals(key) && hashCode.equals(head.hashCode)){
                head.value = value;
                return;
            } else {
                head = head.next;
            }
        }

        //If key not present then insert it to
        size++;
        head = buckets.get(bucketIdx);
        HashNode node = new HashNode(key, value, hashCode);
        node.next = head;
        buckets.set(bucketIdx, node);
    }

    //Hash function for obtaining hash index for a key
    private Integer hashFunction(Integer key){
        Integer hashCode = key.hashCode();
        Integer index = hashCode % numBuckets;
        index = index < 0 ? index * -1 : index;
        return index;
    }

    public ArrayList<HashNode> getBuckets() {
        return buckets;
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
            next = null;
        }
    }
}