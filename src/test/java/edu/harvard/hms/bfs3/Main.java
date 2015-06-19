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

import com.amazonaws.regions.Regions;

import ij.ImageJ;
import ij.ImagePlus;

import java.io.IOException;

import loci.formats.FormatException;
import loci.plugins.BF;

/** Manual test drive of {@link AmazonS3Handle}. */
public class Main {

	public static void main(String... args) throws IOException, FormatException {
		final String bucketName = "dpwr";
		final String key = "s3test/bus.png";
		final Regions regions = Regions.US_EAST_1;


//		final String id = AmazonS3Handle.makeId(bucketName, key, regions);
		final String id = S3Cache.makeId(bucketName, key, regions);

		final ImagePlus[] imps = BF.openImagePlus(id);
//		final ImagePlus[] imps = BF.openImagePlus("/Users/dpwrussell/Downloads/train.jpg");
		new ImageJ();
		for (final ImagePlus imp : imps)
			imp.show();

		
		
//		S3Cache s3 = new S3Cache(bucketName, key, regions);
//		AmazonS3Handle s3b = new AmazonS3Handle(bucketName, key, regions);
				
// Test AmazonS3Handle and S3Cache in conjunction
//		s3.seek(0);
//		byte[] bp = new byte[51];
//		s3.read(bp);
//		
//		s3.seek(50);
//		s3b.seek(50);
//		byte[] b = new byte[5];
//		byte[] bb = new byte[5];
//		s3.read(b);
//		s3b.read(bb);
//		for (byte bi: b) {
//			System.out.print(bi + " ");
//		}
//		System.out.println();
//		for (byte bi: bb) {
//			System.out.print(bi + " ");
//		}

	}

		
		

}
