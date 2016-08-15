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
public class AccessorChainMutatorTest {
	
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
	
	private static MutatorByField<City, String> cityNameMutator;
	private static MutatorByField<Address, City> addressCityMutator;
	private static MutatorByField<Person, Address> personAddressMutator;
	private static MutatorByField<Address, List> addressPhonesMutator;
	private static MutatorByMethod<? extends List, Phone> phoneListMutator;
	private static MutatorByField<Phone, String> phoneNumberMutator;
	private static MutatorByMethod<Phone, String> phoneNumberMethodMutator;
	private static MutatorByMethod<String, Character> charAtMutator;
	private static MutatorByMethod<String, Character[]> toCharArrayMutator;
	private static ArrayMutator<String> charArrayMutator;
	
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
		
		cityNameMutator = Accessors.mutatorByField(City.class, "name");
		addressCityMutator = Accessors.mutatorByField(Address.class, "city");
		personAddressMutator = Accessors.mutatorByField(Person.class, "address");
		addressPhonesMutator = Accessors.mutatorByField(Address.class, "phones");
		phoneListMutator = new ListMutator<>(2);
		phoneNumberMutator = Accessors.mutatorByField(Phone.class, "number");
		phoneNumberMethodMutator = Accessors.mutatorByMethod(Phone.class, "number");
		charAtMutator = new MutatorByMethod<>(Reflections.findMethod(String.class, "charAt", Integer.TYPE));
		toCharArrayMutator = new MutatorByMethod<>(Reflections.findMethod(String.class, "toCharArray"));
		charArrayMutator = new ArrayMutator<>(2);
	}
	
	@DataProvider
	public static Object[][] testGetMutatorData() {
		return new Object[][]{
				{cityNameAccessor, cityNameMutator},
				{addressCityAccessor, addressCityMutator},
				{personAddressAccessor, personAddressMutator},
				{addressPhonesAccessor, addressPhonesMutator},
				{phoneListAccessor, phoneListMutator},
				{phoneNumberAccessor, phoneNumberMutator},
				{phoneNumberMethodAccessor, phoneNumberMutator},
				{charArrayAccessor, charArrayMutator}
			
		};
	}
	
	@DataProvider
	public static Object[][] testGetMutator_exception_data() {
		return new Object[][]{
				{charAtAccessor, charAtMutator},    // chartAt() has no mutator equivalent
				{toCharArrayAccessor, toCharArrayMutator},    // toCharArray() has no mutator equivalent
		};
	}
	
	@Test
	@UseDataProvider("testGetMutatorData")
	public void testGetMutator(IAccessor accessor, IMutator expected) {
		assertEquals(expected, accessor.toMutator());
	}
	
	@Test(expected = IllegalArgumentException.class)
	@UseDataProvider("testGetMutator_exception_data")
	public void testGetMutator_exception(IAccessor accessor, IMutator expected) {
		assertEquals(expected, accessor.toMutator());
	}
	
	public static List<IAccessor> list(IAccessor ... accessors) {
		return Arrays.asList(accessors);
	}
	
	@DataProvider
	public static Object[][] testSetData() {
		return new Object[][] {
				{ list(cityNameAccessor),
						new City("Toto"), "Tata" },
				{ list(addressCityAccessor, cityNameAccessor),
						new Address(new City("Toto"), null), "Tata" },
				{ list(personAddressAccessor, addressCityAccessor, cityNameAccessor),
						new Person(new Address(new City("Toto"), null)), "Tata" },
				{ list(personAddressAccessor, addressPhonesAccessor, phoneListAccessor, phoneNumberAccessor),
						new Person(new Address(null, Arrays.asList(new Phone("123"), new Phone("456"), new Phone("789")))), "000" },
				{ list(personAddressAccessor, addressPhonesAccessor, phoneListAccessor, phoneNumberMethodAccessor),
						new Person(new Address(null, Arrays.asList(new Phone("123"), new Phone("456"), new Phone("789")))), "000" },
				{ list(charArrayAccessor),
						new char[] { '1', '2', '3' }, '0' },
		};
	}
	
	@Test
	@UseDataProvider("testSetData")
	public void testSet(List<IAccessor> accessors, Object object, Object expected) {
		AccessorChain<Object, Object> accessorChain = new AccessorChain<>(accessors);
		AccessorChainMutator testInstance = accessorChain.toMutator();
		testInstance.set(object, expected);
		assertEquals(expected, accessorChain.get(object));
	}
	
	@Test(expected = NullPointerException.class)
	public void testSet_NullPointerException() {
		List<IAccessor> accessors = list(personAddressAccessor, addressPhonesAccessor);
		Object object = new Person(null);
		AccessorChainMutator testInstance = new AccessorChain(accessors).toMutator();
		testInstance.set(object, new Address(new City("Toto"), null));
	}
	
}