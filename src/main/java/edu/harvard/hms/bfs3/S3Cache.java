package edu.harvard.hms.bfs3;

import java.io.IOException;
import java.util.ArrayList;

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
	Long readAHead = new Long(1*1000*1000);

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
		System.out.println("--------------------");
		
		System.out.println("requested: " + requested);
		// Bump up the request size to the readAHead
		if (requested < this.readAHead) {
			requested = this.readAHead;
		}
		System.out.println("requested: " + requested);
		// If the request is for more bytes than there are left in the file
		// Drop down to the remaining number
		if (pos + requested > this.length()) {
			System.out.println("Bailing for length: " + this.length() + " from: " + pos + " requested: " + requested);
			requested = this.length() - pos;
		}
		System.out.println("requested: " + requested);
		// If the request is for more bytes than there are between the position
		// and the next cached block (if there is one)
		// Drop down to the number to fill the gap
		if (nextBlock != null && pos + requested > nextBlock.getValue().getStart()) {
			requested = nextBlock.getValue().getStart() - pos;
		}
		System.out.println("requested: " + requested);
		
		return requested;
	}
	
	// Get the blocks for bytes requested
	public List<Long> getBlocks(int len) throws IOException {
		
		Long start = new Long(this.getFilePointer());
		Long end = new Long(len + this.getFilePointer() -1);
		System.out.println("start: " + start + " end: " + end + " len: " + len + " max: " + this.length() + " available: " + (this.length() - start));
		List<Long> keys = new ArrayList<Long>();
		
		// Loop until enough blocks are found in the cache or requested (and thus added to the cache)
		Long vPos = start;
		while (vPos < end + 1) {
			// Find the next relevant cache block
			Entry<Long, DataValue> entry = cache.floorEntry(vPos);
			
			// If there is no floor key (either there are no cached blocks or the first cached block is after the requested bytes)
			// or the requested bytes start after the cached block 
			if (entry == null || vPos > entry.getValue().getEnd()) {
				// Get the next block (if there is one) to figure how far away it is
				Entry<Long, DataValue> nextBlock = cache.ceilingEntry(vPos);
				
				// Determine how many bytes to retrieve
				Long retrieve = retrieveCount(end - vPos, vPos, nextBlock);
				
				// If there are no bytes to retrieve, BioFormats is reading beyond the end of the file
				// This seems like a bug, but I handle it here simply by bailing from the getBlock operation
				// if retrieveCount determines that there are zero bytes to be read which should only happen
				// if BioFormats is attempting to read beyond the file length
				// TODO File bug?
				if (retrieve == 0) {
					break;
				}
				
				// Get and add this block to the cache
				DataValue dv = this.populateBlock(vPos, retrieve);
				this.cache.put(new Long(vPos), dv);
				keys.add(new Long(vPos));
				vPos += retrieve;
			} else {

				// Add this block to the list of useful blocks
				keys.add(entry.getKey());
				vPos = entry.getValue().getEnd() + 1;
			}
		}
		
		return keys;
	}
	
	private DataValue populateBlock(Long start, Long length) throws IOException {
		sum++;
		System.out.println(sum + " retrieving: " + start + " - " + (start + length - 1) + " (" + length + ")");
		final S3Object object =
				this.getS3().getObject(new GetObjectRequest(this.getBucketName(), this.getKey()).withRange(start, start + length - 1));
		final S3ObjectInputStream stream = object.getObjectContent();
		byte[] b = new byte[(int) (0 + length)];
		
		int read = 0;
		while (read < length) {
			read += stream.read(b, read, (int) (length - read));
		}

		DataValue dv = new DataValue(start, length, b);
		stream.close();
		return dv;
	}

	@Override
	public int read(final byte[] b, final int off, final int len) throws IOException {
		System.out.println("Requesting bytes: " + this.getFilePointer() + " - " + (len + this.getFilePointer()-1) + " len: " + len + " max: " + (this.length() - this.getFilePointer()));
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
