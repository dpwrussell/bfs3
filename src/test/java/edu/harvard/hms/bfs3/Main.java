
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
		final String key = "s3test/bus.tif";
		final Regions regions = Regions.US_EAST_1;

		final String id = AmazonS3Handle.makeId(bucketName, key, regions);

		final ImagePlus[] imps = BF.openImagePlus(id);
		new ImageJ();
		for (final ImagePlus imp : imps)
			imp.show();
	}

}
