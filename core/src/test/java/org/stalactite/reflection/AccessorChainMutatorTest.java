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
public class AccessorChainMutatorTest {
	
	private static final String TEST_GET_MUTATOR_DATA = "testGetMutatorData";
	private static final String TEST_GET_MUTATOR_DATA_EXCEPTION = "testGetMutatorDataException";
	private static final String TEST_SET_DATA = "testSetData";
	
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
	
	private MutatorByField<City, String> cityNameMutator;
	private MutatorByField<Address, City> addressCityMutator;
	private MutatorByField<Person, Address> personAddressMutator;
	private MutatorByField<Address, List> addressPhonesMutator;
	private MutatorByMethod<? extends List, Phone> phoneListMutator;
	private MutatorByField<Phone, String> phoneNumberMutator;
	private MutatorByMethod<Phone, String> phoneNumberMethodMutator;
	private MutatorByMethod<String, Character> charAtMutator;
	private MutatorByMethod<String, Character[]> toCharArrayMutator;
	private ArrayMutator<String> charArrayMutator;
	
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
		
		cityNameMutator = Accessors.mutatorByField(City.class, "name");
		addressCityMutator = Accessors.mutatorByField(Address.class, "city");
		personAddressMutator = Accessors.mutatorByField(Person.class, "address");
		addressPhonesMutator = Accessors.mutatorByField(Address.class, "phones");
		phoneListMutator = new ListMutator<>(2);
		phoneNumberMutator = Accessors.mutatorByField(Phone.class, "number");
		phoneNumberMethodMutator = Accessors.mutatorByMethod(Phone.class, "number");
		charAtMutator = new MutatorByMethod<>(Reflections.getMethod(String.class, "charAt", Integer.TYPE));
		toCharArrayMutator = new MutatorByMethod<>(Reflections.getMethod(String.class, "toCharArray"));
		charArrayMutator = new ArrayMutator<>(2);
	}
	
	@DataProvider(name = TEST_GET_MUTATOR_DATA)
	public Object[][] testGetMutatorData() {
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
	
	@DataProvider(name = TEST_GET_MUTATOR_DATA_EXCEPTION)
	public Object[][] testGetMutator_exception_data() {
		return new Object[][]{
				{charAtAccessor, charAtMutator},    // chartAt() has no mutator equivalent
				{toCharArrayAccessor, toCharArrayMutator},    // toCharArray() has no mutator equivalent
		};
	}
	
	@Test(dataProvider = TEST_GET_MUTATOR_DATA)
	public void testGetMutator(IAccessor accessor, IMutator expected) throws Exception {
		assertEquals(expected, AccessorChainMutator.getMutator(accessor));
	}
	
	@Test(dataProvider = TEST_GET_MUTATOR_DATA_EXCEPTION, expectedExceptions = IllegalArgumentException.class)
	public void testGetMutator_exception(IAccessor accessor, IMutator expected) throws Exception {
		assertEquals(expected, AccessorChainMutator.getMutator(accessor));
	}
	
	public List<IAccessor> list(IAccessor ... accessors) {
		return Arrays.asList(accessors);
	}
	@DataProvider(name = TEST_SET_DATA)
	public Object[][] testSetData() {
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
	
	@Test(dataProvider = TEST_SET_DATA)
	public void testSet(List<IAccessor> accessors, Object object, Object expected) {
		AccessorChain<Object, Object> accessorChain = new AccessorChain<>(accessors);
		AccessorChainMutator testInstance = AccessorChainMutator.toAccessorChainMutator(accessorChain.getAccessors());
		testInstance.set(object, expected);
		assertEquals(expected, accessorChain.get(object));
	}
	
	@Test(expectedExceptions = NullPointerException.class)
	public void testSet_NullPointerException() throws Exception {
		List<IAccessor> accessors = list(personAddressAccessor, addressPhonesAccessor);
		Object object = new Person(null);
		AccessorChainMutator testInstance = AccessorChainMutator.toAccessorChainMutator(accessors);
		testInstance.set(object, new Address(new City("Toto"), null));
	}
	
}