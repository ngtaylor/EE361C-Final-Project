/* CuckooTest.java
 * EE361C Final Project
 */

package cuckoo_hashing;

import org.junit.Assert;
import org.junit.Test;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class CuckooTest {
	
	static int ITERATIONS = 100000;
	static int NUM_THREADS = 8;

    public class TestLockThread implements Runnable{
        LockCuckoo hash;
        public TestLockThread(LockCuckoo hash) {
            this.hash = hash;
        }
        public void run() {
        	/* following array doesn't have any cycles and hence all keys will be inserted without any rehashing */
        	//int keys[] = {20, 50, 53, 75, 100, 67, 105, 3, 36, 39};
			int keys[] = new int[ITERATIONS];
			for (int i = 0; i < ITERATIONS; i++) {
				keys[i] = i;
			}    			
        	int n = keys.length;
        	// start with placing every key at its position in
        	// the first hash table according to first hash
        	// function
        	for (int i = 0, cnt = 0; i < n; i++, cnt = 0)
        		hash.place(keys[i], 0, cnt, n);
        }
    }

    public class TestCoarseLockThread implements Runnable{
        CoarseLockCuckoo hash;
        public TestCoarseLockThread(CoarseLockCuckoo hash) {
            this.hash = hash;
        }
        
        public void run() {
    		/* following array doesn't have any cycles and hence all keys will be inserted without any rehashing */
    		//int keys[] = {20, 50, 53, 75, 100, 67, 105, 3, 36, 39};
        	int keys[] = new int[ITERATIONS];
    		for (int i = 0; i < ITERATIONS; i++) {
    			keys[i] = i;
    		}    			
    		int n = keys.length;
    		// start with placing every key at its position in
    		// the first hash table according to first hash
    		// function
    		for (int i = 0, cnt = 0; i < n; i++, cnt = 0)
    			hash.place(keys[i], 0, cnt, n);
        }
    }
    
    public class TestStripedLockThread implements Runnable{
        StripedLockCuckoo hash;
        public TestStripedLockThread(StripedLockCuckoo hash) {
            this.hash = hash;
        }
        
        public void run() {
    		/* following array doesn't have any cycles and hence all keys will be inserted without any rehashing */
    		//int keys[] = {20, 50, 53, 75, 100, 67, 105, 3, 36, 39};
        	int keys[] = new int[ITERATIONS];
    		for (int i = 0; i < ITERATIONS; i++) {
    			keys[i] = i;
    		}    			
    		int n = keys.length;
    		// start with placing every key at its position in
    		// the first hash table according to first hash
    		// function
    		for (int i = 0, cnt = 0; i < n; i++, cnt = 0)
    			hash.place(keys[i], 0, cnt, n);
        }
    }

    public class TestFreeThread implements Runnable{
        LockFreeCuckoo hash;
        public TestFreeThread(LockFreeCuckoo hash) {
            this.hash = hash;
        }
        public void run() {
    		/* following array doesn't have any cycles and hence all keys will be inserted without any rehashing */
    		//int keys[] = {20, 50, 53, 75, 100, 67, 105, 3, 36, 39};
			int keys[] = new int[ITERATIONS];
			for (int i = 0; i < ITERATIONS; i++) {
				keys[i] = i;
			}   
    		int n = keys.length;
    		// start with placing every key at its position in
    		// the first hash table according to first hash
    		// function
    		for (int i = 0, cnt = 0; i < n; i++, cnt = 0)
    			hash.place(keys[i], 0, cnt, n);
        }
    }

    @Test
    public void testLockCuckoo() throws ExecutionException, InterruptedException {
        LockCuckoo hash = new LockCuckoo();
        LockCuckoo.hashtable = new int[LockCuckoo.ver][LockCuckoo.MAXN];
        LockCuckoo.pos = new int[LockCuckoo.ver];
        LockCuckoo.initTable();
        LockCuckoo.initLocks();
        int numThreads = NUM_THREADS;
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
		// print the final table
		//hash.printTable();
		int count = hash.itemCount();
		System.out.println("lock item count: " + count + "\n");
		Assert.assertTrue("Item count is correct", count == ITERATIONS);
    }

    @Test
    public void testCoarseLockCuckoo() throws ExecutionException, InterruptedException {
        CoarseLockCuckoo hash = new CoarseLockCuckoo();
        CoarseLockCuckoo.hashtable = new int[CoarseLockCuckoo.ver][CoarseLockCuckoo.MAXN];
        CoarseLockCuckoo.pos = new int[CoarseLockCuckoo.ver];
        CoarseLockCuckoo.lock = new ReentrantLock();
        CoarseLockCuckoo.initTable();
        int numThreads = NUM_THREADS;
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
		// print the final table
		//hash.printTable();
		int count = hash.itemCount();
		System.out.println("coarse lock item count: " + count + "\n");
		Assert.assertTrue("Item count is correct", count == ITERATIONS);
    }
    
    @Test
    public void testStripedLockCuckoo() throws ExecutionException, InterruptedException {
    	StripedLockCuckoo hash = new StripedLockCuckoo();
    	StripedLockCuckoo.hashtable = new int[StripedLockCuckoo.ver][StripedLockCuckoo.MAXN];
    	StripedLockCuckoo.pos = new int[StripedLockCuckoo.ver];
    	StripedLockCuckoo.initLocks();
    	StripedLockCuckoo.initTable();
        int numThreads = NUM_THREADS;
        Thread[] threads = new Thread[numThreads];
        for(int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(new TestStripedLockThread(hash));
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
		// print the final table
		//hash.printTable();
		int count = hash.itemCount();
		System.out.println("striped lock item count: " + count + "\n");
		Assert.assertTrue("Item count is correct", count == ITERATIONS);
    }

    @Test
    public void testLockFreeCuckoo() {
        LockFreeCuckoo hash = new LockFreeCuckoo();
        int numThreads = NUM_THREADS;
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
		// print the final table
		//hash.printTable();
		int count = hash.itemCount();
		System.out.println("lock free item count: " + count + "\n");
		Assert.assertTrue("Item count is correct", count == ITERATIONS);
    }

    //This tests a normal chain hash table that does not involve concurrent algorithms
    @Test
    public void testNormalCuckoo(){
        NormalCuckoo hash = new NormalCuckoo();
		/* following array doesn't have any cycles and hence all keys will be inserted without any rehashing */
		//int keys[] = {20, 50, 53, 75, 100, 67, 105, 3, 36, 39};
    	int keys[] = new int[ITERATIONS];
		for (int i = 0; i < ITERATIONS; i++) {
			keys[i] = i;
		}   
		int n = keys.length;
		// start with placing every key at its position in the first hash table according to first hash function
		for (int i = 0, cnt = 0; i < n; i++, cnt = 0)
			hash.place(keys[i], 0, cnt, n);
		// print the final table
		//hash.printTable();
		int count = hash.itemCount();
		System.out.println("normal item count: " + count + "\n");
		Assert.assertTrue("Item count is correct", count == n);		

//        NormalCuckoo hash2 = new NormalCuckoo();
//		/* following array has a cycle and hence we will have to rehash to position every key */
//		int keys_2[] = {20, 50, 53, 75, 100, 67, 105, 3, 36, 39, 6};
//		int m = keys_2.length;
//		// start with placing every key at its position in the first hash table according to first hash function
//		for (int i = 0, cnt = 0; i < m; i++, cnt = 0)
//			hash2.place(keys_2[i], 0, cnt, m);
//		// print the final table
//		hash2.printTable();
//		int count2 = hash.itemCount();
//		Assert.assertTrue("Item count is correct", count2 == m-1);
    }

}
