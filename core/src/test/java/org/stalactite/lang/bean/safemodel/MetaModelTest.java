package org.stalactite.lang.bean.safemodel;

import static org.junit.Assert.assertEquals;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class MetaModelTest {
	
	private static final String TEST_PATH_DATA = "testPathData";
	
	@DataProvider(name = TEST_PATH_DATA)
	public Object[][] testPathData() {
		return new Object[][] {
				{ new MetaPerson<>().address.city, "address.city" },
				{ new MetaPerson<>().address.phones.number, "address.phones.number" },
				{ new MetaPerson<>().address.phones(2).number, "address.phones.get(2).number" },
		};
	}
	
	@Test(dataProvider = TEST_PATH_DATA)
	public void testPath(MetaModel metaModel, String expected) throws Exception {
		assertEquals(expected, MetaModel.path(metaModel));
	}
}