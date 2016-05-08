package org.gama.safemodel;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.safemodel.metamodel.*;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class MetaModelPathComponentBuilderTest {
	
	@DataProvider
	public static Object[][] testTransformData() {
		return new Object[][] {
				{ new MetaPhoneComponent<>().number, "number" },
				{ new MetaAddressComponent<>().phone.number, "phone:number" },
				{ new MetaAddressComponent<>().city.name, "city:name" },
				{ new MetaPersonComponent<>().address.city, "address:city" },
				{ new MetaPersonComponent<>().address.phone.number, "address:phone:number" },
		};
	}
	
	@Test
	@UseDataProvider("testTransformData")
	public void testTransform(MetaModel metaModel, String expected) throws Exception {
		MetaModelPathComponentBuilder testInstance = new MetaModelPathComponentBuilder();
		assertEquals(expected, testInstance.transform(metaModel));
	}
}
