package org.gama.reflection;

import java.util.List;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.Reflections;
import org.gama.lang.collection.Arrays;
import org.gama.reflection.model.Address;
import org.gama.reflection.model.City;
import org.gama.reflection.model.Person;
import org.gama.reflection.model.Phone;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class AccessorChainTest {
	
	private static AccessorByField<City, String> cityNameAccessor;
	private static AccessorByField<Address, City> addressCityAccessor;
	private static AccessorByField<Person, Address> personAddressAccessor;
	private static AccessorByField<Address, List> addressPhonesAccessor;
	private static AccessorByMethod<? extends List, Phone> phoneListAccessor;
	private static AccessorByField<Phone, String> phoneNumberAccessor;
	private static AccessorByMethod<Phone, String> phoneNumberMethodAccessor;
	private static AccessorByMethod<String, Character> charAtAccessor;
	private static AccessorByMethod<String, Character[]> toCharArrayAccessor;
	private static ArrayAccessor<String> charArrayAccessor;
	
	@BeforeClass
	public static void init() {
		cityNameAccessor = Accessors.accessorByField(City.class, "name");
		addressCityAccessor = Accessors.accessorByField(Address.class, "city");
		personAddressAccessor = Accessors.accessorByField(Person.class, "address");
		addressPhonesAccessor = Accessors.accessorByField(Address.class, "phones");
		phoneListAccessor = new ListAccessor<>(2);
		phoneNumberAccessor = Accessors.accessorByField(Phone.class, "number");
		phoneNumberMethodAccessor = Accessors.accessorByMethod(Phone.class, "number");
		charAtAccessor = new AccessorByMethod<>(Reflections.findMethod(String.class, "charAt", Integer.TYPE));
		toCharArrayAccessor = new AccessorByMethod<>(Reflections.findMethod(String.class, "toCharArray"));
		charArrayAccessor = new ArrayAccessor<>(2);
	}
	
	public static List<IAccessor> list(IAccessor ... accessors) {
		return Arrays.asList(accessors);
	}
	
	@DataProvider
	public static Object[][] testGetData() {
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
	
	@Test
	@UseDataProvider("testGetData")
	public void testGet(List<IAccessor> accessors, Object object, Object expected) {
		AccessorChain<Object, Object> accessorChain = new AccessorChain<>(accessors);
		assertEquals(expected, accessorChain.get(object));
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testGet_IllegalArgumentException() throws Exception {
		// field "number" doesn't exist on Collection "phones" => get(..) should throw IllegalArgumentException
		List<IAccessor> accessors = list(personAddressAccessor, addressPhonesAccessor, phoneNumberAccessor);
		Object object = new Person(new Address(null, Arrays.asList(new Phone("123"))));
		AccessorChain<Object, Object > testInstance = new AccessorChain<>(accessors);
		testInstance.get(object);
	}
	
	@Test(expected = NullPointerException.class)
	public void testGet_NullPointerException() throws Exception {
		List<IAccessor> accessors = list(personAddressAccessor, addressPhonesAccessor, phoneNumberAccessor);
		Object object = new Person(new Address(null, null));
		AccessorChain<Object, Object > testInstance = new AccessorChain<>(accessors);
		testInstance.get(object);
	}
}