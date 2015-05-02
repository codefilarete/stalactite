package org.stalactite.reflection;

import static org.junit.Assert.*;

import java.util.List;

import org.stalactite.lang.Reflections;
import org.stalactite.lang.bean.safemodel.model.Address;
import org.stalactite.lang.bean.safemodel.model.City;
import org.stalactite.lang.bean.safemodel.model.Person;
import org.stalactite.lang.bean.safemodel.model.Phone;
import org.stalactite.lang.collection.Arrays;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Guillaume Mary
 */
public class AccessorChainTest {
	
	private static final String TEST_GET_DATA = "testGetData";
	
	private AccessorByField<City, String> cityNameAccessor;
	private AccessorByField<Address, City> addressCityAccessor;
	private AccessorByField<Person, Address> personAddressAccessor;
	private AccessorByField<Address, List> addressPhonesAccessor;
	private AccessorByMethod<? extends List, Phone> phoneListAccessor;
	private AccessorByField<Phone, String> phoneNumberAccessor;
	private AccessorByMethod<Phone, String> phoneNumberMethodAccessor;
	private AccessorByMethod<String, Character> charAtAccessor;
	private AccessorByMethod<String, Character[]> toCharArrayAccessor;
	private ArrayAccessor<String> charArrayAccessor;
	
	@BeforeClass
	public void init() {
		cityNameAccessor = Accessors.accessorByField(City.class, "name");
		addressCityAccessor = Accessors.accessorByField(Address.class, "city");
		personAddressAccessor = Accessors.accessorByField(Person.class, "address");
		addressPhonesAccessor = Accessors.accessorByField(Address.class, "phones");
		phoneListAccessor = new ListAccessor<>(2);
		phoneNumberAccessor = Accessors.accessorByField(Phone.class, "number");
		phoneNumberMethodAccessor = Accessors.accessorByMethod(Phone.class, "number");
		charAtAccessor = new AccessorByMethod<>(Reflections.getMethod(String.class, "charAt", Integer.TYPE));
		toCharArrayAccessor = new AccessorByMethod<>(Reflections.getMethod(String.class, "toCharArray"));
		charArrayAccessor = new ArrayAccessor<>(2);
	}
	
	public List<IAccessor> list(IAccessor ... accessors) {
		return Arrays.asList(accessors);
	}
	
	@DataProvider(name = TEST_GET_DATA)
	public Object[][] testGetData() {
		return new Object[][] {
				{ list(cityNameAccessor),
						new City("Toto"), "Toto" },
				{ list(addressCityAccessor, cityNameAccessor),
						new Address(new City("Toto"), null), "Toto" },
				{ list(personAddressAccessor, addressCityAccessor, cityNameAccessor),
						new Person(new Address(new City("Toto"), null)), "Toto" },
				{ list(personAddressAccessor, addressPhonesAccessor, phoneListAccessor, phoneNumberAccessor),
						new Person(new Address(null, Arrays.asList(new Phone("123"), new Phone("456"), new Phone("789")))), "789" },
				{ list(personAddressAccessor, addressPhonesAccessor, phoneListAccessor, phoneNumberMethodAccessor),
						new Person(new Address(null, Arrays.asList(new Phone("123"), new Phone("456"), new Phone("789")))), "789" },
				{ list(personAddressAccessor, addressPhonesAccessor, phoneListAccessor, phoneNumberMethodAccessor, charAtAccessor.setParameters(2)),
						new Person(new Address(null, Arrays.asList(new Phone("123"), new Phone("456"), new Phone("789")))), '9' },
				{ list(personAddressAccessor, addressPhonesAccessor, phoneListAccessor, phoneNumberMethodAccessor, toCharArrayAccessor, charArrayAccessor),
						new Person(new Address(null, Arrays.asList(new Phone("123"), new Phone("456"), new Phone("789")))), '9' },
				{ list(toCharArrayAccessor, charArrayAccessor),
						"123", '3' },
		};
	}
	
	@Test(dataProvider = TEST_GET_DATA)
	public void testGet(List<IAccessor> accessors, Object object, Object expected) {
		AccessorChain<Object, Object> accessorChain = new AccessorChain<>(accessors);
		assertEquals(expected, accessorChain.get(object));
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testGet_IllegalArgumentException() throws Exception {
		// field "number" doesn't exist on Collection "phones" => get(..) should throw IllegalArgumentException
		List<IAccessor> accessors = list(personAddressAccessor, addressPhonesAccessor, phoneNumberAccessor);
		Object object = new Person(new Address(null, Arrays.asList(new Phone("123"))));
		AccessorChain<Object, Object > accessorChain = new AccessorChain<>(accessors);
		accessorChain.get(object);
	}
}