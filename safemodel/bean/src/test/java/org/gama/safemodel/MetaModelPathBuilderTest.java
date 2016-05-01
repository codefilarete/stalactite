package org.gama.safemodel;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.safemodel.metamodel.MetaPerson;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class MetaModelPathBuilderTest {
	
	@DataProvider
	public static Object[][] testTransformData() {
		return new Object[][] {
				{ new MetaPerson<>().address.city, "address.city" },
				{ new MetaPerson<>().address.phones.number, "address.phones.number" },
				{ new MetaPerson<>().address.phones(2).number, "address.phones.get(2).number" },
				{ new MetaPerson<>().address.phones(2).getNumber(), "address.phones.get(2).getNumber()" },
				{ new MetaPerson<>().address.phones(2).getNumber().charAt(2), "address.phones.get(2).getNumber().charAt(2)" },
				{ new MetaPerson<>().address.phones(2).getNumber().toCharArray(2), "address.phones.get(2).getNumber().toCharArray()[2]" },
				{ new MetaPerson<>().address.phones(2).getNumber().length(), "address.phones.get(2).getNumber().length()" },
		};
	}
	
	@Test
	@UseDataProvider("testTransformData")
	public void testTransform(MetaModel metaModel, String expected) throws Exception {
		MetaModelPathBuilder testInstance = new MetaModelPathBuilder();
		assertEquals(expected, testInstance.transform(metaModel));
	}
}