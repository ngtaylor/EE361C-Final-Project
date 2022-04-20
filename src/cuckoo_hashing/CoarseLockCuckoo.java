package cuckoo_hashing;

import java.util.concurrent.locks.ReentrantLock;

public class CoarseLockCuckoo {
	
	//upper bound on number of elements in our set
	static int MAXN = CuckooTest.ITERATIONS;

	//choices for position
	static int ver = 2;

	//Auxiliary space bounded by a small multiple
	//of MAXN, minimizing wastage
	static int [][]hashtable;

	//Array to store possible positions for a key
	static int []pos;
	
    //Global lock for coarse grained locking of hash table
    static ReentrantLock lock;
	
	/* function to fill hash table with dummy value
	 * dummy value: INT_MIN
	 * number of hashtables: ver */
	static void initTable() {
		for (int j = 0; j < MAXN; j++)
			for (int i = 0; i < ver; i++)
				hashtable[i][j] = Integer.MIN_VALUE;
	}
	
	public CoarseLockCuckoo() {
//		hashtable = new int[ver][MAXN];
//		pos = new int[ver];
//		lock = new ReentrantLock();
//		initTable();
	}
	
	/* return hashed value for a key
	 * function: ID of hash function according to which key has to hashed
	 * key: item to be hashed */
	int hash(int function, int key) {
		switch (function) {
			case 1: return key % MAXN;
			case 2: return (key / MAXN) % MAXN;
		}
		return Integer.MIN_VALUE;
	}
	

	/* function to place a key in one of its possible positions
	 * tableID: table in which key has to be placed, also equal to function according to which key must be hashed
	 * cnt: number of times function has already been called in order to place the first input key
	 * n: maximum number of times function can be recursively called before stopping and declaring presence of cycle */
	void place(int key, int tableID, int cnt, int n) {
		lock.lock();
		try {
			/* if function has been recursively called max number of times, stop and declare cycle. Rehash. */
			if (cnt == n) {
				System.out.printf("%d unpositioned\n", key);
				System.out.printf("Cycle present. REHASH.\n");
				return;
			}

			/* calculate and store possible positions for the key.
			 * check if key already present at any of the positions. 
			 * If YES, return. */
			for (int i = 0; i < ver; i++) {
				pos[i] = hash(i + 1, key);
				if (hashtable[i][pos[i]] == key)
					return;
			}

			/* check if another key is already present at the position for the new key in the table
			 * If YES: place the new key in its position
			 * and place the older key in an alternate position for it in the next table */
			if (hashtable[tableID][pos[tableID]] != Integer.MIN_VALUE) {
				int dis = hashtable[tableID][pos[tableID]];
				hashtable[tableID][pos[tableID]] = key;
				place(dis, (tableID + 1) % ver, cnt + 1, n);
			}
			else // else: place the new key in its position
				hashtable[tableID][pos[tableID]] = key;
		} finally {
			lock.unlock();
		}
	}

	/* function to print hash table contents */
	void printTable() {
		System.out.printf("Final hash tables:\n");

		for (int i = 0; i < ver; i++, System.out.printf("\n"))
			for (int j = 0; j < MAXN; j++)
				if(hashtable[i][j] == Integer.MIN_VALUE)
					System.out.printf("- ");
				else
					System.out.printf("%d ", hashtable[i][j]);

		System.out.printf("\n");
	}
	
	/* function to return the count of items in the hash table */
	int itemCount() {
		int count = 0;
		
		for (int i = 0; i < ver; i++) {
			for (int j = 0; j < MAXN; j++) {
				if(hashtable[i][j] == Integer.MIN_VALUE) {
					// ignore initial values
				} else {
					count++; 
				}	
			}
		}
		return count;
	}
}