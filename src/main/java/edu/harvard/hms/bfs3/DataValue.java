package edu.harvard.hms.bfs3;

import java.lang.Math;

public class DataValue {
	private Long length;
	private Long end;
	private Long start;
	private byte[] data;

	public DataValue(Long start, Long length, byte[] data) {
		super();
		this.start = start;
		this.length = length;
		this.end = start + length - 1;
		this.data = data;
	}

	public Long getStart() {
		return this.start;
	}
	
//	public Long getNextByte(Long start) {
//		if ((this.getStart() <= start) &&
//			(this.getEnd() >= start)) {
//			return Math.max(this.getStart(), start);
//		}
//		return start;
//	}

	public Long getEnd() {
		return this.end;
	}
	
	public Long getLength() {
		return this.length;
	}

	public byte[] getData() {
		return this.data;
	}
	
	public void setData(byte[] b) {
		this.data = b;
	}
	
	public int inRange(Long start, Long end) {
		// If the end of the requested range is before this range
		if (end < this.getStart()) {
			return -1;
		}
		// If the start of the requested range is beyond the end of this range
		else if (start > this.getEnd()) {
			return 1;
		}
		// Otherwise it overlaps
		return 0;
	}
	
	// Write all the bytes that are present in this data value into the given byte array
	public Long getBytes(byte[] b, Long off, Long start, Long num) {
		Long end = start + num - 1;

		// Find where the actual start block is. We know the range has overlap so the start must be present somewhere
		Long actualStart;
		if (start > this.getStart()) {
			actualStart = start;
		} else {
			actualStart = this.getStart();
		}

		// Find the point in this data block that corresponds to the start point
		// This is not the start point in this block, using the discovered range above make it so
		int blockStart = (int) (actualStart - this.getStart());

		// Calculate the number of bytes requested that have already been fulfilled.
		Long done = actualStart - start;
		
		// Copy blocks until there are no more blocks to copy or the target range has been reached
		int pos = 0;
		while ((pos + actualStart <= this.getEnd()) && (pos + actualStart <= end)) {
			b[(int) (off+done+pos)] = this.getData()[blockStart+pos];
			pos++;
		}

		// Return the number of bytes actually read
		return new Long(pos - 1);

	}
}