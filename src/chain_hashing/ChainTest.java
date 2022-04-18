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

        }
    }

    public class TestFreeThread implements Runnable{
        LockFreeChain hash;
        public TestFreeThread(LockFreeChain hash) {
            this.hash = hash;
        }
        public void run() {

        }
    }

    @Test
    public void testLockChain() throws ExecutionException, InterruptedException {
        LockChain hash = new LockChain();
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

    }

    //This tests a normal chain hash table that does not involve concurrent algorithms
    @Test
    public void testNormalChain(){
        LockChain hash = new LockChain();
        hash.put(1,1);
        hash.put(2,2);
        hash.put(1,3);
        hash.put(4,4);
        System.out.println(hash.size);
        System.out.println(hash.remove(1));
        System.out.println(hash.remove(1));
        System.out.println(hash.size);
        System.out.println(hash.isEmpty());
        System.out.println(hash.remove(2));
        System.out.println(hash.remove(4));
        System.out.println(hash.size);
        System.out.println(hash.isEmpty());
    }

}