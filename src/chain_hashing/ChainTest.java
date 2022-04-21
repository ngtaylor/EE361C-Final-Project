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
    static int ITERATIONS = 150;
    static int NUM_THREADS = 1;
    static int NUM_BUCKETS = 100;

    public class TestLockThread implements Runnable{
        LockChain hash;
        public TestLockThread(LockChain hash) {
            this.hash = hash;
        }
        public void run() {
            for (int i = 0; i < ITERATIONS; i++) {
                hash.put(i, i);
            }
            for (int i = 0; i < ITERATIONS; i++) {
                hash.remove(i);
            }
        }
    }

    public class TestCoarseLockThread implements Runnable{
        CoarseLockChain hash;
        public TestCoarseLockThread(CoarseLockChain hash) {
            this.hash = hash;
        }
        public void run() {
            for (int i = 0; i < ITERATIONS; i++) {
                hash.put(i, i);
            }
            for (int i = 0; i < ITERATIONS; i++) {
                hash.remove(i);
            }
        }
    }

    public class TestFreeThread implements Runnable{
        LockFreeChain hash;
        public TestFreeThread(LockFreeChain hash) {
            this.hash = hash;
        }
        public void run() {
            for (int i = 0; i < ITERATIONS; i++) {
                hash.put(i, i);
            }
            for (int i = 0; i < ITERATIONS; i++) {
                hash.remove(i);
            }
        }
    }

    @Test
    public void testLockChain() throws ExecutionException, InterruptedException {
        LockChain hash = new LockChain(NUM_BUCKETS);
        Thread[] threads = new Thread[NUM_THREADS];
        for(int i = 0; i < NUM_THREADS; i++) {
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
        for(int i = 0; i < NUM_BUCKETS; i++) {
            LockChain.HashNode head = buckets.get(i).next;
            while(head != null){
                count++;
                head = head.next;
            }
        }
        int expected = 0;
        System.out.println("# of items in hashtable: " + count + " Expected #: " + expected);
        Assert.assertTrue("Count: " + count + " Expected: " + expected, count == expected);
    }

    @Test
    public void testCoarseLockChain() throws ExecutionException, InterruptedException {
        CoarseLockChain hash = new CoarseLockChain(NUM_BUCKETS);
        Thread[] threads = new Thread[NUM_THREADS];
        for(int i = 0; i < NUM_THREADS; i++) {
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
        for(int i = 0; i < NUM_BUCKETS; i++) {
            CoarseLockChain.HashNode head = buckets.get(i);
            while(head.key != null){
                count++;
                head = head.next;
            }
        }
        int expected = 0;
        System.out.println("# of items in hashtable: " + count + " Expected #: " + expected);
        Assert.assertTrue("Count: " + count + " Expected: " + expected, count == expected);
    }

    @Test
    public void testLockFreeChain() {
        LockFreeChain hash = new LockFreeChain(NUM_BUCKETS);
        Thread[] threads = new Thread[NUM_THREADS];
        for(int i = 0; i < NUM_THREADS; i++) {
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
        for(int i = 0; i < NUM_BUCKETS; i++) {
            AtomicMarkableReference<LockFreeChain.HashNode> head = buckets.get(i).getReference().next;
            while(head != null){
                count++;
                head = head.getReference().next;
            }
        }
        int expected = 0;
        System.out.println("# of items in hashtable: " + count + " Expected #: " + expected);
        Assert.assertTrue("Count: " + count + " Expected: " + expected, count == expected);
    }

    //This tests a normal chain hash table that does not involve concurrent algorithms
    @Test
    public void testNormalChain() {
        NormalChain hash = new NormalChain(NUM_BUCKETS);
        for (int i = 0; i < ITERATIONS; i++) {
            hash.put(i, i);
        }
        for (int i = 0; i < ITERATIONS; i++) {
            hash.remove(i);
        }
        ArrayList<NormalChain.HashNode> buckets = hash.getBuckets();
        int count = 0;
        for (int i = 0; i < NUM_BUCKETS; i++) {
            NormalChain.HashNode head = buckets.get(i).next;
            while (head != null) {
                count++;
                head = head.next;
            }
        }
        int expected = 0;
        System.out.println("# of items in hashtable: " + count + " Expected #: " + expected);
        Assert.assertTrue("Count: " + count + " Expected: " + expected, count == expected);
    }
}