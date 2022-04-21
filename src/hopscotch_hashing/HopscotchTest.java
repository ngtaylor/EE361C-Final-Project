package hopscotch_hashing;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.*;

public class HopscotchTest {
	
	static int ITERATIONS = 1000000;
	static int NUM_THREADS = 16;
	
    public class TestLockThread implements Runnable{
        LockHopscotch hash;
        public TestLockThread(LockHopscotch hash) {
            this.hash = hash;
        }
        
        public void run() {
    		for (int i = 0; i < ITERATIONS; i++) {
    			hash.add(i, i);
    		}  
    		for (int i = 0; i < ITERATIONS; i++) {
    			hash.remove(i);
    		}  
        }
    }
	
    public class TestCoarseLockThread implements Runnable{
        CoarseLockHopscotch hash;
        public TestCoarseLockThread(CoarseLockHopscotch hash) {
            this.hash = hash;
        }
        
        public void run() {
    		for (int i = 0; i < ITERATIONS; i++) {
    			hash.add(i, i);
    		}  
    		for (int i = 0; i < ITERATIONS; i++) {
    			hash.remove(i);
    		}  
        }
    }

    @Test
    public void testLockHopscotch() throws ExecutionException, InterruptedException {
    	LockHopscotch hash = new LockHopscotch();
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
		int count = hash.getItemCount();
		System.out.println("fine-grain lock item count: " + count + "\n");
		Assert.assertTrue("Item count is correct", count == 0);
    }
    
    @Test
    public void testCoarseLockHopscotch() throws ExecutionException, InterruptedException {
    	CoarseLockHopscotch hash = new CoarseLockHopscotch();
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
		int count = hash.getItemCount();
		System.out.println("coarse lock item count: " + count + "\n");
		Assert.assertTrue("Item count is correct", count == 0);
    }
    
    @Test
    public void testNormalHopscotch(){
    	NormalHopscotch hash = new NormalHopscotch();
		for (int i = 0; i < ITERATIONS; i++) {
			hash.add(i, i);
		}  
		for (int i = 0; i < ITERATIONS; i++) {
			hash.remove(i);
		}    	
		int count = hash.getItemCount();
		System.out.println("normal item count: " + count + "\n");
		Assert.assertTrue("Item count is correct", count == 0);
    }
	
}