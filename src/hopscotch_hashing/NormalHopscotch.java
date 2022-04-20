package hopscotch_hashing;

public class NormalHopscotch {
	final static int HOP_RANGE = 32;
	final static int ADD_RANGE = 256;
	final static int MAX_SEGMENTS = 1048576; // Including neighbourhood for last hash location

	Bucket segments[];		// The actual table
	int BUSY;

	/*Bucket is the table object.
	Each bucket contains a key and data pairing (as in a usual hashmap),
	and an "hop_info" variable, containing the information 
	regarding all the keys that were initially mapped to the same bucket.*/
	class Bucket {
	
		long hop_info;
		int key;
		int data;

		//CTR - bucket
		Bucket() {
			hop_info = 0;
			key = -1;
			data = -1;
		}
	}
	
	public NormalHopscotch(){
		segments = new Bucket[MAX_SEGMENTS+256];
		for (int i = 0; i < MAX_SEGMENTS+256; i++) {
			segments[i] = new Bucket();
		}
		BUSY = -1;
	}
	
	// returns the count of objects
	int getItemCount() {
		Bucket temp;
		int count = 0;
		for(int i = 0; i < MAX_SEGMENTS+256; i++) {
			temp = segments[i];
			if(temp.key != -1) {
				count++;
			}

		}
		return count;
	}
	
	/*int remove(int key)
	Key - the key we'd like to remove from the table
	Returns the data paired with key, if the table contained the key,
	and NULL otherwise*/
	int remove(int key) {
		int hash = ((key) & (MAX_SEGMENTS-1));
		Bucket start_bucket = segments[hash];

		long hop_info = start_bucket.hop_info;
		long mask = 1;
		for(int i = 0; i < HOP_RANGE; ++i, mask <<= 1) {
			if((mask & hop_info) >= 1) {
				Bucket check_bucket = segments[hash+i];
				if(key == check_bucket.key) {
					int rc = check_bucket.data;
					check_bucket.key = -1;
					check_bucket.data = -1;
					start_bucket.hop_info &= ~(1<<i);
					return rc;
				}
			}
		}
		return -1;
	} 
	/**
	 * inputs:
	 * 	free_bucket_index - the index of the first empty bucket in the table (not in the neighborhood)
	 * 	free_distance - the function uses this for returns
	 * 	val - the function uses this for returns
	 * 
	 * Return an array - result[0] - free distance, result[1] - val, result[2] - new free bucket
	 * 	"free_distance" : the distance between start_bucket and the newly freed bucket
	 * 	val = 0, if it was able to free a bucket in the neighborhood of start_bucket; otherwise, val remains unchanged
	 * 	new_free_bucket : the index of the newly freed bucket
	 */
	
	int[] find_closer_bucket(int free_bucket_index, int free_distance, int val) {
		//result[0] - free distance, result[1] - val, result[2] - new free bucket
		int[] result = new int[3];
		int move_bucket_index = free_bucket_index - (HOP_RANGE-1);
		Bucket move_bucket = segments[move_bucket_index];
		for(int free_dist = (HOP_RANGE -1); free_dist > 0; --free_dist) {
			long start_hop_info = move_bucket.hop_info;
			int move_free_distance = -1;
			long mask = 1;
			for(int i = 0; i < free_dist; ++i, mask <<= 1) {
				if((mask & start_hop_info) >= 1) {
					move_free_distance = i;
					break;
				}
			}
			/*When a suitable bucket is found, its content is moved to the old free_bucket*/
			if(-1 != move_free_distance) {
				if(start_hop_info == move_bucket.hop_info) {
					int new_free_bucket_index = move_bucket_index + move_free_distance;
					Bucket new_free_bucket = segments[new_free_bucket_index];
					/*Updates move_bucket's hop_info, to indicate the newly inserted bucket*/
					move_bucket.hop_info |= (1 << free_dist);
					segments[free_bucket_index].data = new_free_bucket.data;
					segments[free_bucket_index].key = new_free_bucket.key;
					new_free_bucket.key = BUSY;
					new_free_bucket.data = BUSY;
					/*Updates move_bucket's hop_info, to indicate the deleted bucket*/
					move_bucket.hop_info &= ~(1<<move_free_distance);
					segments[free_bucket_index] = new_free_bucket;
					free_distance = free_distance - free_dist + move_free_distance;
					result[0] = free_distance;
					result[1] = val;
					result[2] = new_free_bucket_index;
					return result;
				}
			}
			++move_bucket_index;
			move_bucket = segments[move_bucket_index];
		}
		segments[free_bucket_index].key = -1;
		result[0] = 0;
		result[1] = 0;
		result[2] = 0;
		return result;
	}

	/**
	 * input: Key - the key to search for in the table
	 * Returns: true if the table contains the key; false otherwise
	 */	
	boolean contains(int key) {
		int hash = ((key) & (MAX_SEGMENTS-1));
		Bucket start_bucket = segments[hash];

		long hop_info = start_bucket.hop_info;
		long mask = 1;
		for(int i = 0; i < HOP_RANGE; ++i, mask <<= 1) {
			if((mask & hop_info) >= 1){
				Bucket check_bucket = segments[hash+i];
				if(key == check_bucket.key) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * inputs: Key, Data - the key and data pair to add to the table.
	 * Returns: true if the operation was successful; false otherwise
	 */	
	boolean add(int key, int data) {
		int val = 1;
		int hash=((key) & (MAX_SEGMENTS-1));
		Bucket start_bucket = segments[hash];
		if(contains(key)) {
			return false;
		}
		/*Looks for a free space to add the new bucket, inside the neighbourhood of start_bucket*/
		int free_bucket_index = hash;
		Bucket free_bucket = segments[hash];
		int free_distance = 0;
		for(; free_distance < ADD_RANGE; ++free_distance) {
			if(-1 == free_bucket.key) {
				free_bucket.key = BUSY;
				break;
			}
			++free_bucket_index;
			free_bucket = segments[free_bucket_index];
		}
		
		//0 - free distance, 1 - val, 2 - new_free_bucket_index
		int[] closest_bucket_info = new int[3];
		if(free_distance < ADD_RANGE) {
			do {
				if(free_distance < HOP_RANGE) {
					/*Inserts the new bucket to the free space*/
					start_bucket.hop_info |= (1<<free_distance);
					free_bucket.data = data;
					free_bucket.key = key;
					return true;
				} else {
					/*In case a free space was not found in the neighborhood of start_bucket,
					Clears such a space*/
					closest_bucket_info = find_closer_bucket(free_bucket_index, free_distance, val);
					free_distance = closest_bucket_info[0];
					val = closest_bucket_info[1];
					free_bucket_index = closest_bucket_info[2];
					free_bucket = segments[free_bucket_index];
				}
			} while(0 != val);
		}
		//System.out.println("Called Resize");
		return false;
	}
}