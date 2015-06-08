package edu.harvard.hms.bfs3;

import java.util.Arrays;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SparseTest {

	public static void readBytes(byte[] results, Long writeOffset, DataValue dv, Long readOffset, Long numBytes) {
		Long n = new Long(0);
		System.out.println("writeOffset: " + writeOffset);
		System.out.println("readOffset: " + readOffset);
		System.out.println("numBytes: " + numBytes);
		while (n < numBytes) {
			System.out.println(n);
			results[(int) (n+writeOffset)] = dv.getData()[(int) (n+readOffset)];
			n++;
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ConcurrentSkipListMap<Long, DataValue> cache = new ConcurrentSkipListMap<Long, DataValue>();

		byte[] dv0bytes = new byte[10];
		Arrays.fill( dv0bytes, (byte) 0 );
		DataValue dv0 = new DataValue(new Long(0), new Long(10), dv0bytes);
		cache.put(new Long(0), dv0);
		
		byte[] dv100bytes = new byte[10];
		Arrays.fill( dv100bytes, (byte) 1 );
		DataValue dv100 = new DataValue(new Long(100), new Long(10), dv100bytes);
		cache.put(new Long(100), dv100);

		byte[] dv200bytes = new byte[10];
		Arrays.fill( dv200bytes, (byte) 2 );
		DataValue dv200 = new DataValue(new Long(200), new Long(10), dv200bytes);
		cache.put(new Long(200), dv200);
		
		byte[] dv300bytes = new byte[10];
		Arrays.fill( dv300bytes, (byte) 3 );
		DataValue dv300 = new DataValue(new Long(300), new Long(10), dv300bytes);
		cache.put(new Long(300), dv300);

		Long start = new Long(99);
		Long end = new Long(220);
		int length = (int) (end - start + 1);

		// Construct a container for the results
		byte[] requestedBytes = new byte[length];

		Long key = cache.floorKey(start);
		Long toKey = cache.floorKey(end);

		Long missingStart = start;
		// Get the blocks which are definitely included in the request
		ConcurrentNavigableMap<Long, DataValue> subcache = cache.subMap(key, true, toKey, true);
		
		for (DataValue dv: subcache.values()) {
			if (dv.getEnd() > missingStart) {
				if (missingStart < dv.getStart()) {
					System.out.println("missing: " + missingStart + " - " + (dv.getStart() - 1));
				}
				Long bytesRead = dv.getBytes(requestedBytes, start, new Long(length));
				missingStart = dv.getEnd() + 1;
			}
		}
		
		if (end > missingStart) {
			System.out.println("missing: " + missingStart + " - " + end);
		}
		
//		for (byte b: requestedBytes) {
//			System.out.println(b);
//		}
	}

}
