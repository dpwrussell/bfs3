/*
 * #%L
 * Amazon S3 support for Bio-Formats.
 * %%
 * Copyright (C) 2015 Harvard University and Board of Regents
 * of the University of Wisconsin-Madison.
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */

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

import loci.common.IRandomAccess;
import loci.common.Location;

/**
 * An {@link IRandomAccess} implementation for Amazon S3 data.
 *
 * @see IRandomAccess
 * @author Douglas Russell
 * @author Curtis Rueden
 */
public class AmazonS3Handle extends AbstractHandle {

	// -- Fields --

	private final String bucketName;
	private final String key;

	private final AmazonS3 s3;
	private final ObjectMetadata objectMetadata;

	/** The current byte offset into the object stream. */
	private long pos;

	// -- Constructors --

	public AmazonS3Handle(final String bucketName, final String key,
		final Regions regions) throws IOException
	{
		this(null, bucketName, key, regions);
	}

	public AmazonS3Handle(final AWSCredentials c,
		final String bucketName, final String key, final Regions regions)
		throws IOException
	{
		super(false);
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
	public void seek(final long pos) throws IOException {
		this.pos = pos;
	}

	@Override
	public void write(final ByteBuffer buf, final int off, final int len)
		throws IOException
	{
		throw new UnsupportedOperationException();
	}

	// -- DataOutput API metthods --

	@Override
	public void write(final byte[] b, final int off, final int len)
		throws IOException
	{
		throw new UnsupportedOperationException();
	}

}
