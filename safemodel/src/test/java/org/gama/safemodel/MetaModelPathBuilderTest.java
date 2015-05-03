package org.gama.safemodel;

import org.gama.safemodel.metamodel.MetaPerson;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class MetaModelPathBuilderTest {
	
	private static final String TEST_PATH_DATA = "testTransformData";
	
	@DataProvider(name = TEST_PATH_DATA)
	public Object[][] testTransformData() {
		return new Object[][] {
				{ new MetaPerson<>().address.city, "address.city" },
				{ new MetaPerson<>().address.phones.number, "address.phones.number" },
				{ new MetaPerson<>().address.phones(2).number, "address.phones.get(2).number" },
				{ new MetaPerson<>().address.phones(2).getNumber(), "address.phones.get(2).getNumber()" },
				{ new MetaPerson<>().address.phones(2).getNumber2().charAt(2), "address.phones.get(2).getNumber().charAt(2)" },
				{ new MetaPerson<>().address.phones(2).getNumber2().toCharArray(2), "address.phones.get(2).getNumber().toCharArray()[2]" },
		};
	}
	
	@Test(dataProvider = TEST_PATH_DATA)
	public void testTransform(MetaModel metaModel, String expected) throws Exception {
		MetaModelPathBuilder testInstance = new MetaModelPathBuilder();
		assertEquals(expected, testInstance.transform(metaModel));
	}
}