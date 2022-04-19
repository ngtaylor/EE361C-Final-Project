/* ChainTest.java
 * EE361C Final Project
 */

package chain_hashing;

import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.concurrent.*;

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

    public class TestFreeThread implements Runnable{
        LockFreeChain hash;
        public TestFreeThread(LockFreeChain hash) {
            this.hash = hash;
        }
        public void run() {
            for(int i = 0; i < 30; i++) {
                hash.put(i, i);
            }
        }
    }

    @Test
    public void testLockChain() throws ExecutionException, InterruptedException {
        LockChain hash = new LockChain(20);
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
        int count = 0;
        Integer val;
        for(int i = 0; i < 60; i++) {
            if(i < 30) {
                val = hash.remove(i);
            } else {
                val = hash.remove(i/2);
            }
            if(val == null){
                break;
            } else {
             count++;
            }
        }
        int expected = 30;
        System.out.println("Count: " + count + " Expected: " + expected);
        Assert.assertTrue("Count: " + count + " Expected: " + expected, count == expected);

    }

    @Test
    public void testLockFreeChain() {
        LockFreeChain hash = new LockFreeChain();
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
        int count = 0;
        Integer val;
        for(int i = 0; i < 60; i++) {
            if(i < 30) {
                val = hash.remove(i);
            } else {
                val = hash.remove(i/2);
            }
            if(val == null){
                break;
            } else {
                count++;
            }
        }
        int expected = 30;
        System.out.println("Count: " + count + " Expected: " + expected);
        Assert.assertTrue("Count: " + count + " Expected: " + expected, count == expected);
    }

    //This tests a normal chain hash table that does not involve concurrent algorithms
    @Test
    public void testNormalChain(){
        //LockFreeChain hash = new LockFreeChain(2);
        //LockChain hash = new LockChain(2);
        NormalChain hash = new NormalChain();
        hash.put(1,1);
        hash.put(2,2);
        hash.put(1,3);
        hash.put(4,4);
        hash.put(5,5);
        System.out.println(hash.size);
        System.out.println(hash.remove(1));
        System.out.println(hash.remove(1));
        System.out.println(hash.size);
        System.out.println(hash.isEmpty());
        System.out.println(hash.remove(2));
        System.out.println(hash.get(5));
        System.out.println(hash.remove(4));
        System.out.println(hash.size);
        System.out.println(hash.isEmpty());
    }

}