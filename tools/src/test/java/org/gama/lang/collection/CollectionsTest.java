package org.gama.lang.collection;

import java.util.List;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class CollectionsTest {
	
	@DataProvider
	public static Object[][] testParcelData() {
		return new Object[][] {
				{ Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 3, Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6), Arrays.asList(7, 8)) },
				{ Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8), 4, Arrays.asList(Arrays.asList(1, 2, 3, 4), Arrays.asList(5, 6, 7, 8)) },
		};
	}
	
	@Test
	@UseDataProvider("testParcelData")
	public void testParcel(List<Integer> integers, int blockSize, List<List<Integer>> expected) throws Exception {
		List<List<Integer>> blocks = Collections.parcel(integers, blockSize);
		assertEquals(expected, blocks);
	}
	
}