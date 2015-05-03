package org.gama.lang.collection;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class CollectionsTest {
	
	private static final String TEST_PARCEL_DATA = "testParcelData";
	
	@DataProvider(name = TEST_PARCEL_DATA)
	public Object[][] testParcelData() {
		return new Object[][] {
				{ Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 3, Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6), Arrays.asList(7, 8)) },
				{ Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 4, Arrays.asList(Arrays.asList(1, 2, 3, 4), Arrays.asList(5, 6, 7, 8)) },
		};
	}
	
	@Test(dataProvider = TEST_PARCEL_DATA)
	public void testParcel(List<Integer> integers, int blockSize, List<List<Integer>> expected) throws Exception {
		List<List<Integer>> blocks = Collections.parcel(integers, blockSize);
		assertEquals(expected, blocks);
	}
	
}