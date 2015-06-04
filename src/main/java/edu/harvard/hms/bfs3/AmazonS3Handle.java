
package edu.harvard.hms.bfs3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import loci.common.DataTools;
import loci.common.IRandomAccess;
import loci.common.Location;

/**
 * An {@link IRandomAccess} implementation for Amazon S3 data.
 *
 * @see IRandomAccess
 * @author Douglas Russell
 * @author Curtis Rueden
 */
public class AmazonS3Handle implements IRandomAccess {

	// -- Fields --

	private final String bucketName;
	private final String key;

	private final AmazonS3 s3;
	private final ObjectMetadata objectMetadata;

	/** The current byte offset into the object stream. */
	private long pos;

	// -- Constructors --

	public AmazonS3Handle() throws IOException {
		// TEMP FOR TESTING
		this("dpwr", "s3test/bus.png", Regions.US_EAST_1);
	}

	public AmazonS3Handle(final String bucketName, final String key,
		final Regions regions) throws IOException
	{
		this(null, bucketName, key, regions);
	}

	public AmazonS3Handle(final AWSCredentials c,
		final String bucketName, final String key, final Regions regions)
		throws IOException
	{
		this.bucketName = bucketName;
		this.key = key;

		s3 = c == null ? new AmazonS3Client() : new AmazonS3Client(c);
		final Region usEast1 = Region.getRegion(regions);
		s3.setRegion(usEast1);

		// get the object metadata
		objectMetadata =
			s3.getObjectMetadata(new GetObjectMetadataRequest(bucketName, key));

		seek(0);
	}

	// -- Static utility methods --

	public static String makeId(final String bucketName, final String key,
		final Regions regions) throws IOException
	{
		final AmazonS3Handle handle = new AmazonS3Handle(bucketName, key, regions);
		final String id = "https://s3.amazonaws.com/" + bucketName + "/" + key;
		Location.mapFile(id, handle);
		return id;
	}

	// -- FileHandle API methods --

	public AmazonS3 getS3() {
		return s3;
	}

	// -- IRandomAccess API methods --

	@Override
	public void close() throws IOException {
		// NB: No action needed.
	}

	@Override
	public long getFilePointer() throws IOException {
		return pos;
	}

	@Override
	public long length() throws IOException {
		return objectMetadata.getContentLength();
	}

	@Override
	public int read(final byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	@Override
	public int read(final byte[] b, final int off, final int len)
		throws IOException
	{
		final S3Object object =
			s3.getObject(new GetObjectRequest(bucketName, key).withRange(pos, pos +
				len));

		final S3ObjectInputStream stream = object.getObjectContent();
		final int r = stream.read(b, off, len);
		stream.close();
		pos += r;
		return r;
	}

	@Override
	public int read(final ByteBuffer buffer) throws IOException {
		return read(buffer, 0, buffer.capacity());
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
	public void seek(final long pos) throws IOException {
		this.pos = pos;
	}

	@Override
	public void write(final ByteBuffer buf) throws IOException {
		write(buf, 0, buf.capacity());
	}

	@Override
	public void write(final ByteBuffer buf, final int off, final int len)
		throws IOException
	{
		throw new UnsupportedOperationException();
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
		return DataTools.bytesToDouble(bytes, false);
	}

	@Override
	public float readFloat() throws IOException {
		final byte[] bytes = new byte[4];
		readFully(bytes);
		return DataTools.bytesToFloat(bytes, false);
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
		return DataTools.bytesToInt(bytes, false);
	}

	@Override
	public String readLine() throws IOException {
		throw new UnsupportedOperationException(); //FIXME
	}

	@Override
	public long readLong() throws IOException {
		final byte[] bytes = new byte[8];
		readFully(bytes);
		return DataTools.bytesToLong(bytes, false);
	}

	@Override
	public short readShort() throws IOException {
		final byte[] bytes = new byte[2];
		readFully(bytes);
		return DataTools.bytesToShort(bytes, false);
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
		throw new UnsupportedOperationException();
	}

	@Override
	public void write(final byte[] b, final int off, final int len)
		throws IOException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void write(final int b) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeBoolean(final boolean v) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeByte(final int v) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeBytes(final String s) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeChar(final int v) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeChars(final String s) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeDouble(final double v) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeFloat(final float v) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeInt(final int v) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeLong(final long v) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeShort(final int v) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeUTF(final String str) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public ByteOrder getOrder() {
		return null;
	}

	@Override
	public void setOrder(final ByteOrder order) {
	}

}
