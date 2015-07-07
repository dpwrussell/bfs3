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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import loci.formats.FormatException;
import loci.plugins.BF;
import loci.plugins.in.ImporterOptions;

/** Manual test drive of {@link AmazonS3Handle}. */
public class Main {

	public static void main(String... args) throws IOException, FormatException {
		final String bucketName = "dpwr";
//		final String key = "s3test/bus.png";
//		final String key = "s3test/bus.tif";
//		final String key = "s3test/IN_01.r3d_D3D.dv";
//		final String key = "s3test/hs.tif";
		final String key = "s3test/03302014-r1.nd.ome.tif";
		final Regions regions = Regions.US_EAST_1;


//		final String id = AmazonS3Handle.makeId(bucketName, key, regions);
//		final String id = S3Cache.makeId(bucketName, key, regions);
//		final String id = "/Users/dpwrussell/Downloads/TestData/ometif/03302014-r1.nd.ome.tif";
		
//		ImporterOptions options = new ImporterOptions();
//		options.setId(id);
//		options.setVirtual(true);
		
//		final ImagePlus[] imps = BF.openImagePlus(options);
//		final ImagePlus[] imps = BF.openImagePlus("/Users/dpwrussell/Downloads/TestData/ometif/03302014-r1.nd.ome.tif");
//		final ImagePlus[] imps = BF.openImagePlus("/Users/dpwrussell/Downloads/TestData/tif/bus.tif");
//		final ImagePlus[] imps = BF.openImagePlus("/Users/dpwrussell/Downloads/TestData/tif/hs.tif");
		
//		new ImageJ();
//		for (final ImagePlus imp : imps)
//			imp.show();
		
		S3Cache s3 = new S3Cache(bucketName, key, regions);
//		AmazonS3Handle s3 = new AmazonS3Handle(bucketName, key, regions);
				
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
		
//		byte[] allbytes = new byte[(int) s3.length()];
//		
//		int n = 0;
//		while (n < (int) s3.length()) {
//			n += s3.read(allbytes, n, 1000000);
//			
//		}
		
//		s3.read(allbytes);
		
		// hs5 is 100000bytes per request - OK
		// hs6 is using readFully
//		OutputStream out = new FileOutputStream("/Users/dpwrussell/Downloads/TestData/tif/hs6.tif");
//		out.write(allbytes);
//		out.flush();
//		out.close();
		
//		int into = 51506137;
//		
//		s3.seek(into);
//		byte[] cbytes = new byte[10];
//		s3.read(cbytes);
//		
//		System.out.println();
//		System.out.print("C: ");
//		for (int i=0; i < 10; i++) {
//			System.out.print(cbytes[i] + " ");
//		}
//		
//		System.out.println();
//		System.out.print("A: ");
//		for (int i=0; i < 10; i++) {
//			System.out.print(allbytes[i+into] + " ");
//		}
//		System.out.println();
//		System.out.print("F: ");
//		Path path = Paths.get("/Users/dpwrussell/Downloads/TestData/ometif/03302014-r1.nd.ome.tif");
//		byte[] fileBytes = Files.readAllBytes(path);
//		for (int i=0; i < 10; i++) {
//			System.out.print(fileBytes[i+into] + " ");
//		}
/*
		// Test reading from disk
		long start = System.currentTimeMillis();
		Path path = Paths.get("/Users/dpwrussell/Downloads/TestData/ometif/03302014-r1.nd.ome.tif");
		byte[] b = Files.readAllBytes(path);
		int sum = 0;
		for (byte bi: b) {
			sum += bi;
		}
		long end = System.currentTimeMillis();
		System.out.println("Read file from disk: " + (end-start) + "ms - " + sum);
		System.exit(1);
		// Test reading file in several different size chunks
		// 1 chunk
		start = System.currentTimeMillis();
		S3Cache s3 = new S3Cache(bucketName, key, regions);
		b = new byte[(int) s3.length()];
		s3.read(b);
		end = System.currentTimeMillis();
		System.out.println("Read file in one shot: " + (end-start) + "ms");
		s3.printCache();

		// 10 chunks
		start = System.currentTimeMillis();
		s3 = new S3Cache(bucketName, key, regions);
		b = new byte[(int) s3.length()];
		int read = 0;
		while (read < s3.length()) {
			read += s3.read(b, read, (int) (s3.length()/10));
		}
		
		end = System.currentTimeMillis();
		System.out.println("Read file in 10 chunks" + (end-start) + "ms");		
		s3.printCache();
		
		// 100ish chunks
		start = System.currentTimeMillis();
		s3 = new S3Cache(bucketName, key, regions);
		b = new byte[(int) s3.length()];
		read = 0;
		while (read < s3.length()) {
//			System.out.println(read);
//			read += s3.read(b, read, (int) (s3.length()/100));
			read += s3.read(b, read, 8770000);
		}
		
		end = System.currentTimeMillis();
		System.out.println("Read file in 100 chunks" + (end-start) + "ms");
		s3.printCache();
	*/	
		
		long start = System.currentTimeMillis();
		// 658 1k blocks
		byte[] full = new byte[(int) s3.length()];
		int pos = 0;
		for (int i=0; i<658; i++) {
			pos += s3.read(full, pos, 1024);
		}
		long end = System.currentTimeMillis();
		System.out.println("658 1k reads " + (end-start) + "ms");
	}

		
		

}
