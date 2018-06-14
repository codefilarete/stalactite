package org.gama.sql.result;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.lang.trace.ModifiableInt;
import org.junit.jupiter.api.Test;

import static org.gama.sql.binder.DefaultResultSetReaders.INTEGER_READER;
import static org.gama.sql.binder.DefaultResultSetReaders.STRING_READER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/**
 * @author Guillaume Mary
 */
public class ResultSetRowConverterTest {
	
	@Test
	public void testConvert_basicUseCase() throws SQLException {
		// Binding column "vehicleType" to the constructor of Car
		ResultSetRowConverter<String, Car> testInstance = new ResultSetRowConverter<>(Car.class, "vehicleType", STRING_READER, Car::new);
		// Adding a column to fill the bean
		testInstance.add("wheels", INTEGER_READER, Car::setWheelCount);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("vehicleType", (Object) "bicycle").add("wheels", 2),
				Maps.asMap("vehicleType", (Object) "moto").add("wheels", 2),
				Maps.asMap("vehicleType", (Object) "car").add("wheels", 4),
				Maps.asMap("vehicleType", (Object) "car").add("wheels", 6)
		));
		
		resultSet.next();
		Car vehicle1 = testInstance.convert(resultSet);
		assertEquals("bicycle", vehicle1.getName());
		assertEquals(2, vehicle1.getWheelCount());
		resultSet.next();
		Car vehicle2 = testInstance.convert(resultSet);
		assertEquals("moto", vehicle2.getName());
		assertEquals(2, vehicle2.getWheelCount());
		resultSet.next();
		Car vehicle3 = testInstance.convert(resultSet);
		assertEquals("car", vehicle3.getName());
		assertEquals(4, vehicle3.getWheelCount());
		resultSet.next();
		Car vehicle4 = testInstance.convert(resultSet);
		assertEquals("car", vehicle4.getName());
		// vehicle3 and vehicle4 shouldn't be the same despite their identical name
		assertNotSame(vehicle4, vehicle3);
	}
	
	@Test
	public void testConvert_basicUseCase2() throws SQLException {
		// The default ModifiableInt that takes its value from "a". Reinstanciated on each row.
		ResultSetRowConverter<Integer, ModifiableInt> testInstance = new ResultSetRowConverter<>(ModifiableInt.class, "a", INTEGER_READER, ModifiableInt::new);
		// The secondary that will increment the same ModifiableInt by column "b" value
		testInstance.add(new ColumnConsumer<>("b", INTEGER_READER, (t, i) -> t.increment(Objects.preventNull(i, 0))));
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("a", (Object) 42).add("b", 1),
				Maps.asMap("a", (Object) 666).add("b", null)
		));
		
		resultSet.next();
		assertEquals(43, testInstance.convert(resultSet).getValue());
		resultSet.next();
		// no change on this one because "b" column is null on the row and we took null into account during incrementation
		assertEquals(666, testInstance.convert(resultSet).getValue());
	}
	
	/**
	 * A test based on an {@link ModifiableInt} that would take its value from a {@link java.sql.ResultSet}
	 */
	@Test
	public void testConvert_shareInstanceOverRows() throws SQLException {
		// The default ModifiableInt that takes its value from "a". Shared over rows (class attribute)
		ModifiableInt sharedInstance = new ModifiableInt(0);
		ResultSetRowConverter<Integer, ModifiableInt> testInstance = new ResultSetRowConverter<>(ModifiableInt.class, "a", INTEGER_READER, i -> {
			sharedInstance.increment(i);
			return sharedInstance;
		});
		// The secondary that will increment the same ModifiableInt by column "b" value
		testInstance.add(new ColumnConsumer<>("b", INTEGER_READER, (t, i) -> sharedInstance.increment(Objects.preventNull(i, 0))));
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("a", (Object) 42).add("b", 1),
				Maps.asMap("a", (Object) 666).add("b", null)
		));
		
		resultSet.next();
		assertEquals(43, testInstance.convert(resultSet).getValue());
		resultSet.next();
		// no change on this one because "b" column is null on the row and we took null into account during incrementation
		assertEquals(709, testInstance.convert(resultSet).getValue());
	}
	
	
	@Test
	public void testCopyWithMapping() throws SQLException {
		ResultSetRowConverter<Integer, ModifiableInt> sourceInstance = new ResultSetRowConverter<>(ModifiableInt.class, "a", INTEGER_READER, ModifiableInt::new);
		sourceInstance.add(new ColumnConsumer<>("b", INTEGER_READER, (t, i) -> t.increment(Objects.preventNull(i, 0))));
		
		// we're making our copy with column "a" is now "x", and column "b" is now "y"
		ResultSetRowConverter<Integer, ModifiableInt> testInstance = sourceInstance.copyWithMapping(Maps.asHashMap("a", "x").add("b", "y"));
		
		// of course ....
		assertNotSame(sourceInstance, testInstance);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("x", (Object) 42).add("y", 1),
				Maps.asMap("x", (Object) 666).add("y", null)
		));
		
		resultSet.next();
		assertEquals(43, testInstance.convert(resultSet).getValue());
		resultSet.next();
		// no change on this one because "b" column is null on the row and we took null into account during incrementation
		assertEquals(666, testInstance.convert(resultSet).getValue());
	}
	
	@Test
	public void testCopyFor() throws SQLException {
		ResultSetRowConverter<String, Vehicle> sourceInstance = new ResultSetRowConverter<>(Vehicle.class, "name", STRING_READER, Vehicle::new);
		
		ResultSetRowConverter<String, Car> testInstance = sourceInstance.copyFor(Car.class, Car::new);
		testInstance.add(new ColumnConsumer<>("wheels", INTEGER_READER, Car::setWheelCount));
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("name", (Object) "peugeot").add("wheels", 4)
		));
		
		resultSet.next();
		Car result = testInstance.convert(resultSet);
		assertEquals("peugeot", result.getName());
		assertEquals(4, result.getWheelCount());
	}
	
	@Test
	public void testAddCollection() throws SQLException {
		ResultSetRowConverter<String, Person> testInstance = new ResultSetRowConverter<>(Person.class, "name", STRING_READER, Person::new);
		
		testInstance.add("address1", STRING_READER, Person::getAddresses, Person::setAddresses, ArrayList::new);
		testInstance.add("address2", STRING_READER, Person::getAddresses, Person::setAddresses, ArrayList::new);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("name", (Object) "paul").add("address1", "rue Vaudirard").add("address2", "rue Menon")
		));
		
		resultSet.next();
		Person result = testInstance.convert(resultSet);
		assertEquals("paul", result.getName());
		assertEquals(Arrays.asList("rue Vaudirard", "rue Menon"), result.getAddresses());
	}
	
	private static class Vehicle {
		
		private String name;
		
		private Vehicle(String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
	}
	
	private static class Car extends Vehicle {
		
		private int wheelCount;
		
		private Car(String name) {
			super(name);
		}
		
		public int getWheelCount() {
			return wheelCount;
		}
		
		public void setWheelCount(int wheelCount) {
			this.wheelCount = wheelCount;
		}
	}
	
	static class Person {
		
		private String name;
		
		private List<String> addresses = new ArrayList<>();
		
		Person(String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
		
		public List<String> getAddresses() {
			return addresses;
		}
		
		public void setAddresses(List<String> addresses) {
			this.addresses = addresses;
		}
	}
}