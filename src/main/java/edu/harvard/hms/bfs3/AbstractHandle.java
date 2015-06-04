
package edu.harvard.hms.bfs3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import loci.common.DataTools;
import loci.common.IRandomAccess;

/**
 * Abstract superclass for {@link IRandomAccess} implementations.
 *
 * @see IRandomAccess
 * @author Curtis Rueden
 */
public abstract class AbstractHandle implements IRandomAccess {

	private final boolean little;

	public AbstractHandle(final boolean little) {
		this.little = little;
	}

	// -- IRandomAccess API methods --

	@Override
	public int read(final byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	@Override
	public int read(final ByteBuffer buffer) throws IOException {
		return read(buffer, 0, buffer.remaining());
	}

	@Override
	public int read(final ByteBuffer buffer, final int off, final int len)
		throws IOException
	{
		final byte[] b = new byte[len];
		final int n = read(b);
		buffer.put(b, off, len);
		return n;
	}

	@Override
	public void write(final ByteBuffer buf) throws IOException {
		write(buf, 0, buf.remaining());
	}

	// -- DataInput API methods --

	@Override
	public boolean readBoolean() throws IOException {
		return readByte() != 0;
	}

	@Override
	public byte readByte() throws IOException {
		final byte[] buf = new byte[1];
		readFully(buf);
		return buf[0];
	}

	@Override
	public char readChar() throws IOException {
		return (char) readShort(); // TODO double check
	}

	@Override
	public double readDouble() throws IOException {
		final byte[] bytes = new byte[8];
		readFully(bytes);
		return DataTools.bytesToDouble(bytes, little);
	}

	@Override
	public float readFloat() throws IOException {
		final byte[] bytes = new byte[4];
		readFully(bytes);
		return DataTools.bytesToFloat(bytes, little);
	}

	@Override
	public void readFully(final byte[] b) throws IOException {
		readFully(b, 0, b.length);
	}

	@Override
	public void readFully(final byte[] b, final int off, final int len)
		throws IOException
	{
		int remain = len;
		int read = 0;
		while (remain > 0) {
			final int r = read(b, off + read, remain);
			if (r < 0) throw new IOException("EOF");// TODO double check this
			// TODO verify that r can never be 0 (probably not)
			read += r;
			remain -= r;
		}
	}

	@Override
	public int readInt() throws IOException {
		final byte[] bytes = new byte[4];
		readFully(bytes);
		return DataTools.bytesToInt(bytes, little);
	}

	@Override
	public String readLine() throws IOException {
		throw new UnsupportedOperationException(); //FIXME
	}

	@Override
	public long readLong() throws IOException {
		final byte[] bytes = new byte[8];
		readFully(bytes);
		return DataTools.bytesToLong(bytes, little);
	}

	@Override
	public short readShort() throws IOException {
		final byte[] bytes = new byte[2];
		readFully(bytes);
		return DataTools.bytesToShort(bytes, little);
	}

	@Override
	public int readUnsignedByte() throws IOException {
		return 0xff & readByte();
	}

	@Override
	public int readUnsignedShort() throws IOException {
		return 0xffff & readShort();
	}

	@Override
	public String readUTF() throws IOException {
		throw new UnsupportedOperationException(); //FIXME
	}

	@Override
	public int skipBytes(final int n) throws IOException {
		seek(getFilePointer() + n);
		return n;
	}

	// -- DataOutput API metthods --

	@Override
	public void write(final byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	@Override
	public void write(final int b) throws IOException {
		write(DataTools.intToBytes(b, little));
	}

	@Override
	public void writeBoolean(final boolean v) throws IOException {
		write(v ? (byte) 1 : (byte) 0);
	}

	@Override
	public void writeByte(final int v) throws IOException {
		write((byte) v);
	}

	@Override
	public void writeBytes(String arg0) throws IOException {
		throw new UnsupportedOperationException(); //FIXME
	}

	@Override
	public void writeChar(final int v) throws IOException {
		writeShort(v);
	}

	@Override
	public void writeChars(String arg0) throws IOException {
		throw new UnsupportedOperationException(); //FIXME
	}

	@Override
	public void writeDouble(final double v) throws IOException {
		write(DataTools.doubleToBytes(v, little));
	}

	@Override
	public void writeFloat(final float v) throws IOException {
		write(DataTools.floatToBytes(v, little));
	}

	@Override
	public void writeInt(final int v) throws IOException {
		write(DataTools.intToBytes(v, little));
	}

	@Override
	public void writeLong(final long v) throws IOException {
		write(DataTools.longToBytes(v, little));
	}

	@Override
	public void writeShort(final int v) throws IOException {
		write(DataTools.intToBytes(v, little));
	}

	@Override
	public void writeUTF(String arg0) throws IOException {
		throw new UnsupportedOperationException(); //FIXME
	}

	@Override
	public ByteOrder getOrder() {
		return null;
	}

	@Override
	public void setOrder(final ByteOrder order) {
	}

}
