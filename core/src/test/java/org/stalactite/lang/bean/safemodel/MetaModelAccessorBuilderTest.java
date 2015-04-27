package org.stalactite.lang.bean.safemodel;

import static org.junit.Assert.*;

import java.util.List;

import org.stalactite.lang.bean.safemodel.metamodel.MetaAddress;
import org.stalactite.lang.bean.safemodel.metamodel.MetaCity;
import org.stalactite.lang.bean.safemodel.metamodel.MetaPerson;
import org.stalactite.lang.bean.safemodel.metamodel.MetaString;
import org.stalactite.lang.bean.safemodel.model.Address;
import org.stalactite.lang.bean.safemodel.model.City;
import org.stalactite.lang.bean.safemodel.model.Person;
import org.stalactite.lang.bean.safemodel.model.Phone;
import org.stalactite.lang.collection.Arrays;
import org.stalactite.reflection.IAccessor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class MetaModelAccessorBuilderTest {
	
	private static final String TEST_PATH_DATA = "testTransformData";
	
	@DataProvider(name = TEST_PATH_DATA)
	public Object[][] testTransformData() {
		return new Object[][] {
				{ new MetaCity<>().name, new City("Toto"), "Toto" },
				{ new MetaAddress<>().city.name, new Address(new City("Toto"), null), "Toto" },
				{ new MetaPerson<>().address.city.name, new Person(new Address(new City("Toto"), null)), "Toto" },
				{ new MetaPerson<>().address.phones(2).number, new Person(new Address(null, Arrays.asList(new Phone("123"), new Phone("456"), new Phone("789")))), "789" },
				{ new MetaPerson<>().address.phones(2).getNumber(), new Person(new Address(null, Arrays.asList(new Phone("123"), new Phone("456"), new Phone("789")))), "789" },
				{ new MetaPerson<>().address.phones(2).getNumber2().charAt(2), new Person(new Address(null, Arrays.asList(new Phone("123"), new Phone("456"), new Phone("789")))), '9' },
				{ new MetaPerson<>().address.phones(2).getNumber2().charAt_array(2), new Person(new Address(null, Arrays.asList(new Phone("123"), new Phone("456"), new Phone("789")))), '9' },
				{ new MetaString().charAt_array(2), "123", '3' },
		};
	}
	
	@Test(dataProvider = TEST_PATH_DATA)
	public void testTransform(MetaModel metaModel, Object object, Object expected) throws Exception {
		MetaModelAccessorBuilder testInstance = new MetaModelAccessorBuilder();
		List<IAccessor> accessors = testInstance.transform(metaModel);
		Object target = object;
		for (IAccessor accessor : accessors) {
			target = accessor.get(target);
		}
		assertEquals(expected, target);
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testTransform_IllegalArgumentException() throws Exception {
		MetaModel metaModel = new MetaPerson<>().address.phones.number;
		Object object = new Person(new Address(null, Arrays.asList(new Phone("0123456789"))));
		MetaModelAccessorBuilder testInstance = new MetaModelAccessorBuilder();
		List<IAccessor> accessors = testInstance.transform(metaModel);
		Object target = object;
		for (IAccessor accessor : accessors) {
			target = accessor.get(target);
		}
	}
}