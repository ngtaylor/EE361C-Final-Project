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

    private static int MAX_VALUE = 2147483647;

    public LockFreeChain(){
        buckets = new ArrayList<>();
        numBuckets = 10;
        size = new AtomicInteger();

        //Make empty chains
        for(int i = 0; i < numBuckets; i++){
            //add head sentinel and tail sentinel
            HashNode headNode =  new HashNode(null, null, null);
            HashNode tailNode = new HashNode(MAX_VALUE, MAX_VALUE, MAX_VALUE);
            AtomicMarkableReference<HashNode> tail = new AtomicMarkableReference<HashNode>(tailNode, false);
            headNode.next = tail;
            AtomicMarkableReference<HashNode> head = new AtomicMarkableReference<HashNode>(headNode, false);
            buckets.add(head);
            buckets.add(tail);
        }
    }

    public LockFreeChain(Integer numBuckets){
        this.numBuckets = numBuckets;
        buckets = new ArrayList<>();
        size = new AtomicInteger();

        //Make empty chains
        for(int i = 0; i < numBuckets; i++){
            //add head sentinel and tail sentinel
            HashNode headNode =  new HashNode(null, null, null);
            HashNode tailNode = new HashNode(MAX_VALUE, MAX_VALUE, MAX_VALUE);
            AtomicMarkableReference<HashNode> tail = new AtomicMarkableReference<HashNode>(tailNode, false);
            headNode.next = tail;
            AtomicMarkableReference<HashNode> head = new AtomicMarkableReference<HashNode>(headNode, false);
            buckets.add(head);
            buckets.add(tail);
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

        boolean[] marked = {false};
        HashNode curr = head.getReference().next.getReference();


        while(curr != null && curr.key < key){
            curr = curr.next.getReference();
            HashNode succ = curr.next.get(marked);
        }

        if(curr != null && curr.key == key && !marked[0]){
            return curr.value;
        } else {
            return null;
        }
    }

    //Removes key and returns value associated with it
    public Integer remove(Integer key){
        //Use hash function to get index of key
        Integer bucketIdx = hashFunction(key);
        Integer hashCode = key.hashCode();
        boolean snip;

        //Start at head of chain
        AtomicMarkableReference<HashNode> head = buckets.get(bucketIdx);
        while(true){
            Window window = find(head.getReference(), key);
            HashNode prev = window.prev, curr = window.curr;
            if(curr.key != key){
                return null;
            } else {
                HashNode succ = curr.next.getReference();
                snip = curr.next.attemptMark(succ, true);
                if (!snip) {
                    continue;
                }
                prev.next.compareAndSet(curr, succ, false, false);
                size.decrementAndGet();
                return curr.value;
            }
        }

    }

    //Add key-value pair to hash table
    public void put(Integer key, Integer value){
        //Use hash function to get index of key
        Integer bucketIdx = hashFunction(key);
        Integer hashCode = key.hashCode();

        //Start at head of chain
        AtomicMarkableReference<HashNode> head = buckets.get(bucketIdx);

        //See if key is already in its chain
        while(true){
            Window window = find(head.getReference(), key);
            HashNode prev = window.prev, curr = window.curr;
            //If found break, else keep traversing chain
            if(curr.key == key){
                curr.value = value;
                HashNode node  = new HashNode(curr.key, value, key.hashCode());
                node.next = curr.next;
                if(prev.next.compareAndSet(curr, node, false, false)){
                    return;
                }
            } else {
                size.incrementAndGet();
                HashNode node = new HashNode(key, value, key.hashCode());
                node.next = new AtomicMarkableReference<>(curr, false);
                if(prev.next.compareAndSet(curr, node, false, false)){
                    return;
                }
            }
        }
    }

    //Hash function for obtaining hash index for a key
    private Integer hashFunction(Integer key){
        Integer hashCode = key.hashCode();
        Integer index = hashCode % numBuckets;
        index = index < 0 ? index * -1 : index;
        return index;
    }

    //Inner class used for lock free concurrency
    public class Window {
        public HashNode prev, curr;
        Window(HashNode prev, HashNode curr){
            this.prev = prev;
            this.curr = curr;
        }
    }

    //Returns Window with prev: the node with the largest key less than key, and curr: node with least key greater than or equal to key
    public Window find(HashNode head, int key){
        HashNode prev = null, curr = null, succ = null;
        boolean[] marked = {false};
        boolean snip;
        retry: while(true){
            prev = head;
            curr = prev.next.getReference();
            while(true) {
                //If curr is null then item is not in list
                if(curr.key != MAX_VALUE) {
                    succ = curr.next.get(marked);
                    while (marked[0]) {
                        snip = prev.next.compareAndSet(curr, succ, false, false);
                        if (!snip) continue retry;
                        curr = succ;
                        succ = curr.next.get(marked);
                    }
                }
                if(curr.key >= key){
                    return new Window(prev, curr);
                }
                prev = curr;
                curr = succ;
            }
        }
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
            next = null;
        }
    }
}