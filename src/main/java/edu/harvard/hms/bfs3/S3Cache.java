package edu.harvard.hms.bfs3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import loci.common.Location;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class S3Cache extends AmazonS3Handle {
	private ConcurrentNavigableMap<Long, DataValue> cache;
	int sum;
	Long readAHead = new Long(100);

	public S3Cache(AWSCredentials c, String bucketName, String key,
			Regions regions) throws IOException {
		super(c, bucketName, key, regions);
		this.cache = new ConcurrentSkipListMap<Long, DataValue>();
		sum = 0;
	}
	
	public S3Cache(String bucketName, String key, Regions regions)
			throws IOException {
		super(bucketName, key, regions);
		this.cache = new ConcurrentSkipListMap<Long, DataValue>();
		sum = 0;
	}
	
	// -- Static utility methods --

	public static String makeId(final String bucketName, final String key,
		final Regions regions) throws IOException
	{
		final S3Cache handle = new S3Cache(bucketName, key, regions);
		final String id = "https://s3.amazonaws.com/" + bucketName + "/" + key;
		Location.mapFile(id, handle);
		return id;
	}

	// Calculate how many bytes to retrieve
	private Long retrieveCount(Long requested, Long pos, Entry<Long, DataValue> nextBlock) throws IOException {

		// Bump up the request size to the readAHead
		if (requested < this.readAHead) {
			requested = this.readAHead;
		}

		// If the request is for more bytes than there are left in the file
		// Drop down to the remaining number
		if (pos + requested > this.length()) {
			requested = this.length() - pos;
		}

		// If the request is for more bytes than there are between the position
		// and the next cached block (if there is one)
		// Drop down to the number to fill the gap
		if (nextBlock != null && pos + requested > nextBlock.getValue().getStart()) {
			requested = nextBlock.getValue().getStart() - 1;
		}
		return requested;
	}
	
	// Get the blocks for bytes requested
	public List<Long> getBlocks(int len) throws IOException {
		
		Long start = new Long(this.getFilePointer());
		Long end = new Long(len + this.getFilePointer() -1);
		
		List<Long> keys = new ArrayList<Long>();
		
		// Loop until enough blocks are found in the cache or requested (and thus added to the cache)
		Long vPos = start;
		while (vPos < end + 1) {
			// Find the next relevant cache block
			Entry<Long, DataValue> entry = cache.floorEntry(vPos);
			
			// If there is no floor key or the requested bytes start before the cached block 
			if (entry == null || vPos < entry.getValue().getStart()) {
				System.out.println("They fell before or there were none");
				// Get the next block (if there is one) to figure how far away it is
				Entry<Long, DataValue> nextBlock = cache.ceilingEntry(vPos);
				
				// Determine how many bytes to retrieve
				Long retrieve = retrieveCount(end - vPos, vPos, nextBlock);
				
				// Get and add this block to the cache
				System.out.println("Adding " + retrieve + " bytes to the cache at location " + vPos);
				DataValue dv = this.populateBlock(vPos, retrieve);
				this.cache.put(new Long(vPos), dv);
				keys.add(new Long(vPos));
				vPos += retrieve;
				System.out.println("vPos: " + vPos);
			}
			// If the requested bytes starts after the cached block
			else if (vPos > entry.getValue().getEnd()) {
//				System.out.println("They fell after");
			} else {
				System.out.println("They overlapped");
				
				// Add this block to the list of useful blocks
				keys.add(entry.getKey());
				vPos = entry.getValue().getEnd(); //TODO +1 ?
				System.out.println(vPos + " - " + end);
			}
		}
		
		return keys;
//
//		
//		
//		// There may be no matching keys if the requested range starts before an existing cache block
//		// Or the entire block may be before the range 
//		if (key == null || this.cache.get(key).inRange(start, end) == false) {
//			key = cache.higherKey(start);
//			if (key != null && this.cache.get(key).inRange(start, end) == false) {
//				key = null;
//			}
//		}
//		// If the key is still null then there are no cache blocks at all
//		if (key == null) {
//			// Set the set of matching keys to be empty
//			subcache = new ConcurrentSkipListMap<Long, DataValue>();
//		}
//		
//		
//		
//
//		
//		
//		
//
//		
//		// Find the existing cache blocks within the range of the read
//		Long key = cache.floorKey(start);
//		Long toKey = cache.floorKey(end);
//		
//		// Container for the useful cache blocks
//		ConcurrentNavigableMap<Long, DataValue> subcache;
//		
//		// There may be no matching keys if the requested range starts before an existing cache block
//		// Or the entire block may be before the range 
//		if (key == null || this.cache.get(key).inRange(start, end) == false) {
//			key = cache.higherKey(start);
//			if (key != null && this.cache.get(key).inRange(start, end) == false) {
//				key = null;
//			}
//		}
//		// If the key is still null then there are no cache blocks at all
//		if (key == null) {
//			// Set the set of matching keys to be empty
//			subcache = new ConcurrentSkipListMap<Long, DataValue>();
//		}
//		// Otherwise get all these useful cache blocks
//		else {
//			subcache = cache.subMap(key, true, toKey, true);
//		}
//
//		// Start at the lowest desired position and fill in either cache blocks or placeholders
//		Long pos = start;
//		
//		// Get an iterator for the cacheBlocks
//		//TODO This is backed by the underlying map, so perhaps I should get the keys and look those up as I add blocks to the map during the following operations
//		Iterator<DataValue> cacheBlocks = subcache.values().iterator();
//		DataValue nextDataValue = null;
//		if (cacheBlocks.hasNext()) {
//			nextDataValue = cacheBlocks.next();
//		}
//		
//		List<Long> keys = new ArrayList<Long>();
//		
//		while (pos <= end) {
//			// If there are no remaining cacheBlocks, create a placeholder which runs to the end
//			// of the requested bytes
//			if (nextDataValue == null) {
//				DataValue dv = this.populateBlock(pos, end-pos+1);
//				this.cache.put(new Long(pos), dv);
//				keys.add(new Long(pos));
//				// Set to end+1 so that we escape the while loop
//				pos = end+1;
//			}
//			// If the next available block starts at a location higher than the current position
//			// Create a placeholder to fill this gap
//			else if (nextDataValue.getStart() > pos) {
//				DataValue dv = this.populateBlock(pos, nextDataValue.getStart()-pos);
//				this.cache.put(new Long(pos), dv);
//				keys.add(new Long(pos));
//				pos = nextDataValue.getStart();
//			}
//			// If the current position is inside an existing cache block
//			// Advance to the next block
//			else {
//				keys.add(nextDataValue.getStart());
//				pos = nextDataValue.getEnd() + 1;
//				if (cacheBlocks.hasNext()) {
//					nextDataValue = cacheBlocks.next();
//				} else {
//					nextDataValue = null;
//				}
//			}
//		}
//
//		// Return the keys that correspond to the cache blocks and placeholders
//		return keys;
	}
	
	private DataValue populateBlock(Long start, Long length) throws IOException {
		sum++;
		System.out.println(sum);
		final S3Object object =
				this.getS3().getObject(new GetObjectRequest(this.getBucketName(), this.getKey()).withRange(start, start + length - 1));
		final S3ObjectInputStream stream = object.getObjectContent();
		byte[] b = new byte[(int) (0 + length)];
		stream.read(b, 0, (int) (0 + length));
		DataValue dv = new DataValue(start, length, b);
		stream.close();
		return dv;
	}

	@Override
	public int read(final byte[] b, final int off, final int len) throws IOException {

		// Get the blocks that correspond to the requested range
		List<Long> keys = this.getBlocks(len);

		for (Long key: keys) {
			DataValue dv = this.cache.get(key);
			dv.getBytes(b, new Long(off), this.getFilePointer(), new Long(len));
		}

		// Move the file pointer
		this.setFilePointer(this.getFilePointer() + len);

		return len;
	}
	
	public void printCache() {
		System.out.println("Blocks");
		for (DataValue dv: cache.values()) {
			System.out.println("  " + dv.getStart() + " - " + dv.getEnd());
		}
	}
}
