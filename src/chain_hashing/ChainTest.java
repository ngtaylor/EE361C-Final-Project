/* ChainTest.java
 * EE361C Final Project
 */

package chain_hashing;

import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;

public class ChainTest {

    public class TestLockThread implements Runnable{
        LockChain hash;
        public TestLockThread(LockChain hash) {
            this.hash = hash;
        }
        public void run() {
            for(int i = 0; i < 30; i++) {
                hash.put(i, i);
            }
        }
    }

    public class TestCoarseLockThread implements Runnable{
        CoarseLockChain hash;
        public TestCoarseLockThread(CoarseLockChain hash) {
            this.hash = hash;
        }
        public void run() {
            for(int i = 0; i < 30; i++) {
                hash.put(i, i);
            }
        }
    }

    public class TestFreeThread implements Runnable{
        LockFreeChain hash;
        public TestFreeThread(LockFreeChain hash) {
            this.hash = hash;
        }
        public void run() {
            for(int i = 0; i < 5; i++) {
                hash.put(i, i);
            }
        }
    }

    @Test
    public void testLockChain() throws ExecutionException, InterruptedException {
        int numBuckets = 10;
        LockChain hash = new LockChain(numBuckets);
        int numThreads = 8;
        Thread[] threads = new Thread[numThreads];
        for(int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(new TestLockThread(hash));
            threads[i].start();
        }
        // finish threads
        for(Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //Go through all chains to see if the correct number of items are present
        int count = 0;
        ArrayList<LockChain.HashNode> buckets = hash.getBuckets();
        for(int i = 0; i < numBuckets; i++) {
            LockChain.HashNode head = buckets.get(i).next;
            while(head != null){
                count++;
                head = head.next;
            }
        }
        int expected = 30;
        System.out.println("# of items in hashtable: " + count + " Expected #: " + expected);
        Assert.assertTrue("Count: " + count + " Expected: " + expected, count == expected);

        Integer val;
        for(int i = 0; i < 30; i++){
            val = hash.remove(i);
            System.out.println("Removed values are correct");
            Assert.assertTrue("Removed values are correct", val == i);
        }
    }

    @Test
    public void testCoarseLockChain() throws ExecutionException, InterruptedException {
        int numBuckets = 10;
        CoarseLockChain hash = new CoarseLockChain(numBuckets);
        int numThreads = 8;
        Thread[] threads = new Thread[numThreads];
        for(int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(new TestCoarseLockThread(hash));
            threads[i].start();
        }
        // finish threads
        for(Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //Go through all chains to see if the correct number of items are present
        int count = 0;
        ArrayList<CoarseLockChain.HashNode> buckets = hash.getBuckets();
        for(int i = 0; i < numBuckets; i++) {
            CoarseLockChain.HashNode head = buckets.get(i);
            while(head.key != null){
                count++;
                head = head.next;
            }
        }
        int expected = 30;
        System.out.println("# of items in hashtable: " + count + " Expected #: " + expected);
        Assert.assertTrue("Count: " + count + " Expected: " + expected, count == expected);

        //Now ensure that all the items have the correct values
        Integer val;
        for(int i = 0; i < 30; i++){
            val = hash.remove(i);
            System.out.println("Removed values are correct");
            Assert.assertTrue("Removed values are correct", val == i);
        }
    }

    @Test
    public void testLockFreeChain() {
        int numBuckets = 4;
        LockFreeChain hash = new LockFreeChain(numBuckets);
        int numThreads = 8;
        Thread[] threads = new Thread[numThreads];
        for(int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(new TestFreeThread(hash));
            threads[i].start();
        }
        // finish threads
        for(Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //Go through all chains to see if the correct number of items are present
        int count = 0;
        ArrayList<AtomicMarkableReference<LockFreeChain.HashNode>> buckets = hash.getBuckets();
        for(int i = 0; i < numBuckets; i++) {
            AtomicMarkableReference<LockFreeChain.HashNode> head = buckets.get(i).getReference().next;
            while(head != null){
                count++;
                head = head.getReference().next;
            }
        }
        int expected = 5;
        System.out.println("# of items in hashtable: " + count + " Expected #: " + expected);
        Assert.assertTrue("Count: " + count + " Expected: " + expected, count == expected);

        Integer val;
        for(int i = 0; i < 4; i++){
            val = hash.remove(i);
            System.out.println("Removed values are correct");
            Assert.assertTrue("Removed values are correct", val == i);
        }
    }

    //This tests a normal chain hash table that does not involve concurrent algorithms
    @Test
    public void testNormalChain(){
        int numBuckets = 2;
        LockFreeChain hash = new LockFreeChain(numBuckets);
        //LockChain hash = new LockChain(2);
        //NormalChain hash = new NormalChain();
        //CoarseLockChain hash = new CoarseLockChain(2);
        hash.put(1,1);
        hash.put(2,2);
        hash.put(1,3);
        hash.put(4,4);
        hash.put(5,5);
        int count = 0;
        ArrayList<AtomicMarkableReference<LockFreeChain.HashNode>> buckets = hash.getBuckets();
        for(int i = 0; i < numBuckets; i++) {
            AtomicMarkableReference<LockFreeChain.HashNode> head = buckets.get(i).getReference().next;
            while(head != null){
                count++;
                head = head.getReference().next;
            }
        }
        int expected = 4;
        System.out.println("# of items in hashtable: " + count + " Expected #: " + expected);
        Assert.assertTrue("Count: " + count + " Expected: " + expected, count == expected);

        System.out.println("Size: " + hash.size);
        System.out.println("Remove 1: " + hash.remove(1));
        System.out.println("Remove 1: " + hash.remove(1));
        System.out.println("Size: " + hash.size);
        System.out.println("Remove 2: " + hash.remove(2));
        System.out.println("Get 5: " + hash.get(5));
        System.out.println("Remove 4: " + hash.remove(4));
        System.out.println("Size: " + hash.size);
        System.out.println("Remove 5: " + hash.remove(5));
        System.out.println("Size: " + hash.size);
        System.out.println("isEmpty: " + hash.isEmpty());
    }

}