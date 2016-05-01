package org.gama.safemodel;

import java.util.ArrayList;
import java.util.List;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.Reflections;
import org.gama.lang.collection.Arrays;
import org.gama.reflection.*;
import org.gama.safemodel.lang.MetaString;
import org.gama.safemodel.metamodel.MetaAddress;
import org.gama.safemodel.metamodel.MetaCity;
import org.gama.safemodel.metamodel.MetaPerson;
import org.gama.safemodel.model.Address;
import org.gama.safemodel.model.City;
import org.gama.safemodel.model.Person;
import org.gama.safemodel.model.Phone;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class MetaModelAccessorBuilderTest {
	
	private static AccessorByField<City, String> cityNameAccessor;
	private static AccessorByField<Address, City> addressCityAccessor;
	private static AccessorByField<Person, Address> personAddressAccessor;
	private static AccessorByField<Address, List> addressPhonesAccessor;
	private static AccessorByMethod<List, Phone> phoneListAccessor;
	private static AccessorByField<Phone, String> phoneNumberAccessor;
	private static AccessorByMethod<Phone, String> phoneNumberMethodAccessor;
	private static AccessorByMethod<String, Character> charAtAccessor;
	private static AccessorByMethod<String, Character[]> toCharAccessor;
	private static ArrayAccessor<String> charAtArrayAccessor;
	
	@BeforeClass
	public static void init() {
		cityNameAccessor = Accessors.accessorByField(City.class, "name");
		addressCityAccessor = Accessors.accessorByField(Address.class, "city");
		personAddressAccessor = Accessors.accessorByField(Person.class, "address");
		addressPhonesAccessor = Accessors.accessorByField(Address.class, "phones");
		phoneListAccessor = new AccessorByMethod<>(Reflections.findMethod(List.class, "get", Integer.TYPE));
		phoneNumberAccessor = Accessors.accessorByField(Phone.class, "number");
		phoneNumberMethodAccessor = Accessors.accessorByMethod(Phone.class, "number");
		charAtAccessor = new AccessorByMethod<>(Reflections.findMethod(String.class, "charAt", Integer.TYPE));
		toCharAccessor = new AccessorByMethod<>(Reflections.findMethod(String.class, "toCharArray"));
		charAtArrayAccessor = new ArrayAccessor<>(0);
	}
	
	public static List<IAccessor> list(IAccessor ... accessors) {
		return Arrays.asList(accessors);
	}
	
	@DataProvider
	public static Object[][] testTransformData() {
		return new Object[][] {
				{ new MetaCity<>().name,
						list(cityNameAccessor) },
				{ new MetaAddress<>().city.name,
						list(addressCityAccessor, cityNameAccessor) },
				{ new MetaPerson<>().address.city.name,
						list(personAddressAccessor, addressCityAccessor, cityNameAccessor) },
				{ new MetaPerson<>().address.phones(2).number,
						list(personAddressAccessor, addressPhonesAccessor, phoneListAccessor, phoneNumberAccessor) },
				{ new MetaPerson<>().address.phones(2).getNumber(),
						list(personAddressAccessor, addressPhonesAccessor, phoneListAccessor, phoneNumberMethodAccessor) },
				{ new MetaPerson<>().address.phones(2).getNumber().charAt(2),
						list(personAddressAccessor, addressPhonesAccessor, phoneListAccessor, phoneNumberMethodAccessor, charAtAccessor) },
				{ new MetaPerson<>().address.phones(2).getNumber().toCharArray(2),
						list(personAddressAccessor, addressPhonesAccessor, phoneListAccessor, phoneNumberMethodAccessor, toCharAccessor, charAtArrayAccessor) },
				{ new MetaString<>().toCharArray(2),
						list(toCharAccessor, charAtArrayAccessor) },
		};
	}
	
	@Test
	@UseDataProvider("testTransformData")
	public void testTransform(MetaModel metaModel, List<IAccessor> expected) throws Exception {
		MetaModelAccessorBuilder<Object, Object> testInstance = new MetaModelAccessorBuilder<>();
		AccessorChain<Object, Object> accessorChain = testInstance.transform(metaModel);
		List<IAccessor> accessors = new ArrayList<>();
		for (IAccessor accessor : accessorChain.getAccessors()) {
			accessors.add(accessor);
		}
		assertEquals(expected, accessors);
	}
}