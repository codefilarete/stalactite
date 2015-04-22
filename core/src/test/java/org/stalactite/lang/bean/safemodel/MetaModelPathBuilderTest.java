package org.stalactite.lang.bean.safemodel;

import static org.junit.Assert.*;

import org.stalactite.lang.bean.safemodel.metamodel.MetaPerson;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class MetaModelPathBuilderTest {
	
	private static final String TEST_PATH_DATA = "testPathData";
	
	@DataProvider(name = TEST_PATH_DATA)
	public Object[][] testPathData() {
		return new Object[][] {
				{ new MetaPerson<>().address.city, "address.city" },
				{ new MetaPerson<>().address.phones.number, "address.phones.number" },
				{ new MetaPerson<>().address.phones(2).number, "address.phones.get(2).number" },
				{ new MetaPerson<>().address.phones(2).getNumber(), "address.phones.get(2).getNumber()" },
		};
	}
	
	@Test(dataProvider = TEST_PATH_DATA)
	public void testPath(MetaModel metaModel, String expected) throws Exception {
		MetaModelPathBuilder testInstance = new MetaModelPathBuilder();
		assertEquals(expected, testInstance.transform(metaModel));
	}
}