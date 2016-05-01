package org.gama.safemodel;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.wicket.model.ChainingModel;
import org.gama.lang.collection.Arrays;
import org.gama.safemodel.metamodel.MetaAddress;
import org.gama.safemodel.metamodel.MetaCity;
import org.gama.safemodel.metamodel.MetaPerson;
import org.gama.safemodel.model.Address;
import org.gama.safemodel.model.City;
import org.gama.safemodel.model.Person;
import org.gama.safemodel.model.Phone;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class MetaModelIModelBuilderTest {
	
	@DataProvider
	public static Object[][] testTransformData() {
		return new Object[][] {
				{ new MetaCity<>().name,
						new City("Toto"), "Toto" },
				{ new MetaAddress<>().city.name,
						new Address(new City("Toto"), null), "Toto" },
				{ new MetaPerson<>().address.city.name,
						new Person(new Address(new City("Toto"), null)), "Toto" },
				{ new MetaPerson<>().address.phones(2).number,
						new Person(new Address(null, Arrays.asList(new Phone("123"), new Phone("456"), new Phone("789")))), "789" },
				{ new MetaPerson<>().address.phones(2).getNumber(),
						new Person(new Address(null, Arrays.asList(new Phone("123"), new Phone("456"), new Phone("789")))), "789" },
		};
	}
	
	@Test
	@UseDataProvider("testTransformData")
	public void testTransform(MetaModel metaModel, Object object, Object expected) throws Exception {
		MetaModelIModelBuilder<Object> testInstance = new MetaModelIModelBuilder<>(object);
		ChainingModel<Object> chainingModel = testInstance.transform(metaModel);
		// getObject() check
		assertEquals(expected, chainingModel.getObject());
		// setObject() check
		// NB: all objects are Strings so we can set String values
		chainingModel.setObject("Tata");
		assertEquals("Tata", chainingModel.getObject());
	}
}