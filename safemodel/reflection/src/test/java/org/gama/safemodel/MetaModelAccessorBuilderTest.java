package org.gama.safemodel;

import org.gama.lang.Reflections;
import org.gama.lang.collection.Arrays;
import org.gama.reflection.*;
import org.gama.safemodel.metamodel.MetaAddress;
import org.gama.safemodel.metamodel.MetaCity;
import org.gama.safemodel.metamodel.MetaPerson;
import org.gama.safemodel.metamodel.MetaString;
import org.gama.safemodel.model.Address;
import org.gama.safemodel.model.City;
import org.gama.safemodel.model.Person;
import org.gama.safemodel.model.Phone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class MetaModelAccessorBuilderTest {
	
	private static final String TEST_PATH_DATA = "testTransformData";
	
	private AccessorByField<City, String> cityNameAccessor;
	private AccessorByField<Address, City> addressCityAccessor;
	private AccessorByField<Person, Address> personAddressAccessor;
	private AccessorByField<Address, List> addressPhonesAccessor;
	private AccessorByMethod<List, Phone> phoneListAccessor;
	private AccessorByField<Phone, String> phoneNumberAccessor;
	private AccessorByMethod<Phone, String> phoneNumberMethodAccessor;
	private AccessorByMethod<String, Character> charAtAccessor;
	private AccessorByMethod<String, Character[]> toCharAccessor;
	private ArrayAccessor<String> charAtArrayAccessor;
	
	@BeforeClass
	public void init() {
		cityNameAccessor = Accessors.accessorByField(City.class, "name");
		addressCityAccessor = Accessors.accessorByField(Address.class, "city");
		personAddressAccessor = Accessors.accessorByField(Person.class, "address");
		addressPhonesAccessor = Accessors.accessorByField(Address.class, "phones");
		phoneListAccessor = new AccessorByMethod<>(Reflections.getMethod(List.class, "get", Integer.TYPE));
		phoneNumberAccessor = Accessors.accessorByField(Phone.class, "number");
		phoneNumberMethodAccessor = Accessors.accessorByMethod(Phone.class, "number");
		charAtAccessor = new AccessorByMethod<>(Reflections.getMethod(String.class, "charAt", Integer.TYPE));
		toCharAccessor = new AccessorByMethod<>(Reflections.getMethod(String.class, "toCharArray"));
		charAtArrayAccessor = new ArrayAccessor<>(0);
	}
	
	public List<IAccessor> list(IAccessor ... accessors) {
		return Arrays.asList(accessors);
	}
	
	@DataProvider(name = TEST_PATH_DATA)
	public Object[][] testTransformData() {
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
				{ new MetaString().toCharArray(2),
						list(toCharAccessor, charAtArrayAccessor) },
		};
	}
	
	@Test(dataProvider = TEST_PATH_DATA)
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