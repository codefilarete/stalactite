package org.gama.stalactite.sql.result;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.lang.trace.ModifiableInt;
import org.junit.jupiter.api.Test;

import static org.gama.stalactite.sql.binder.DefaultResultSetReaders.INTEGER_READER;
import static org.gama.stalactite.sql.binder.DefaultResultSetReaders.STRING_READER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/**
 * @author Guillaume Mary
 */
public class ResultSetRowTransformerTest {
	
	@Test
	public void testTransform_basicUseCase() throws SQLException {
		// Binding column "vehicleType" to the constructor of Car
		ResultSetRowTransformer<String, Car> testInstance = new ResultSetRowTransformer<>(Car.class, "vehicleType", STRING_READER, Car::new);
		// Adding a column to fill the bean
		testInstance.add("wheels", INTEGER_READER, Car::setWheelCount);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("vehicleType", (Object) "bicycle").add("wheels", 2),
				Maps.asMap("vehicleType", (Object) "moto").add("wheels", 2),
				Maps.asMap("vehicleType", (Object) "car").add("wheels", 4),
				Maps.asMap("vehicleType", (Object) "car").add("wheels", 6)
		));
		
		resultSet.next();
		Car vehicle1 = testInstance.transform(resultSet);
		assertEquals("bicycle", vehicle1.getName());
		assertEquals(2, vehicle1.getWheelCount());
		resultSet.next();
		Car vehicle2 = testInstance.transform(resultSet);
		assertEquals("moto", vehicle2.getName());
		assertEquals(2, vehicle2.getWheelCount());
		resultSet.next();
		Car vehicle3 = testInstance.transform(resultSet);
		assertEquals("car", vehicle3.getName());
		assertEquals(4, vehicle3.getWheelCount());
		resultSet.next();
		Car vehicle4 = testInstance.transform(resultSet);
		assertEquals("car", vehicle4.getName());
		// vehicle3 and vehicle4 shouldn't be the same despite their identical name
		assertNotSame(vehicle4, vehicle3);
	}
	
	@Test
	public void testTransform_basicUseCase_complexConsumer() throws SQLException {
		// The default ModifiableInt that takes its value from "a". Reinstanciated on each row.
		ResultSetRowTransformer<Integer, ModifiableInt> testInstance = new ResultSetRowTransformer<>(ModifiableInt.class, "a", INTEGER_READER, ModifiableInt::new);
		// The secondary that will increment the same ModifiableInt by column "b" value
		testInstance.add(new ColumnConsumer<>("b", INTEGER_READER, (t, i) -> t.increment(Objects.preventNull(i, 0))));
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class).add("a", 42).add("b", 1),
				Maps.forHashMap(String.class, Object.class).add("a", 666).add("b", null)
		));
		
		resultSet.next();
		assertEquals(43, testInstance.transform(resultSet).getValue());
		resultSet.next();
		// no change on this one because "b" column is null on the row and we took null into account during incrementation
		assertEquals(666, testInstance.transform(resultSet).getValue());
	}
	
	/**
	 * A test based on an {@link ModifiableInt} that would take its value from a {@link java.sql.ResultSet}
	 */
	@Test
	public void testTransform_shareInstanceOverRows() throws SQLException {
		// The default ModifiableInt that takes its value from "a". Shared over rows (class attribute)
		ModifiableInt sharedInstance = new ModifiableInt(0);
		ResultSetRowTransformer<Integer, ModifiableInt> testInstance = new ResultSetRowTransformer<>(ModifiableInt.class, "a", INTEGER_READER, i -> {
			sharedInstance.increment(i);
			return sharedInstance;
		});
		// The secondary that will increment the same ModifiableInt by column "b" value
		testInstance.add(new ColumnConsumer<>("b", INTEGER_READER, (t, i) -> t.increment(Objects.preventNull(i, 0))));
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class).add("a", 42).add("b", 1),
				Maps.forHashMap(String.class, Object.class).add("a", 666).add("b", null)
		));
		
		resultSet.next();
		assertEquals(43, testInstance.transform(resultSet).getValue());
		resultSet.next();
		// no change on this one because "b" column is null on the row and we took null into account during incrementation
		assertEquals(709, testInstance.transform(resultSet).getValue());
	}
	
	
	@Test
	public void testCopyWithAliases() throws SQLException {
		ResultSetRowTransformer<Integer, ModifiableInt> sourceInstance = new ResultSetRowTransformer<>(ModifiableInt.class, "a", INTEGER_READER, ModifiableInt::new);
		sourceInstance.add(new ColumnConsumer<>("b", INTEGER_READER, (t, i) -> t.increment(Objects.preventNull(i, 0))));
		
		// we're making our copy with column "a" is now "x", and column "b" is now "y"
		ResultSetRowTransformer<Integer, ModifiableInt> testInstance = sourceInstance.copyWithAliases(Maps.asHashMap("a", "x").add("b", "y"));
		
		// of course ....
		assertNotSame(sourceInstance, testInstance);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class).add("x", 42).add("y", 1),
				Maps.forHashMap(String.class, Object.class).add("x", 666).add("y", null)
		));
		
		resultSet.next();
		assertEquals(43, testInstance.transform(resultSet).getValue());
		resultSet.next();
		// no change on this one because "b" column is null on the row and we took null into account during incrementation
		assertEquals(666, testInstance.transform(resultSet).getValue());
	}
	
	@Test
	public void copyFor() throws SQLException {
		ResultSetRowTransformer<String, Vehicle> sourceInstance = new ResultSetRowTransformer<>(Vehicle.class, "name", STRING_READER, Vehicle::new);
		sourceInstance.add(new ColumnConsumer<>("color", STRING_READER, Vehicle::setColor));
		
		ResultSetRowTransformer<String, Car> testInstance = sourceInstance.copyFor(Car.class, Car::new);
		testInstance.add(new ColumnConsumer<>("wheels", INTEGER_READER, Car::setWheelCount));
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class).add("name", "peugeot").add("wheels", 4).add("color", "red")
		));
		
		resultSet.next();
		Car result = testInstance.transform(resultSet);
		assertEquals("peugeot", result.getName());
		assertEquals(4, result.getWheelCount());
		assertEquals("red", result.getColor());
	}
	
	@Test
	public void exampleWithCollection() throws SQLException {
		ResultSetRowTransformer<String, Person> testInstance = new ResultSetRowTransformer<>(Person.class, "name", STRING_READER, Person::new);
		
		testInstance.add("address1", STRING_READER, Person::getAddresses, Person::setAddresses, ArrayList::new);
		testInstance.add("address2", STRING_READER, Person::getAddresses, Person::setAddresses, ArrayList::new);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, String.class).add("name", "paul").add("address1", "rue Vaugirard").add("address2", "rue Menon")
		));
		
		resultSet.next();
		Person result = testInstance.transform(resultSet);
		assertEquals("paul", result.getName());
		assertEquals(Arrays.asSet("rue Vaugirard", "rue Menon"), new HashSet<>(result.getAddresses()));
	}
	
	@Test
	public void relation() throws SQLException {
		ResultSetRowTransformer<String, Person> testInstance = new ResultSetRowTransformer<>(Person.class, "name", STRING_READER, Person::new);
		
		ResultSetRowTransformer<String, String> addressTransformer = new ResultSetRowTransformer<>(String.class, "address1", STRING_READER, Function.identity());
		testInstance.add(Person::addAddress, addressTransformer);
		testInstance.add(Person::addAddress, addressTransformer.copyWithAliases(s -> "address2"));
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, String.class).add("name", "paul").add("address1", "rue Vaugirard").add("address2", "rue Menon")
		));
		
		resultSet.next();
		Person result = testInstance.transform(resultSet);
		assertEquals("paul", result.getName());
		assertEquals(Arrays.asSet("rue Vaugirard", "rue Menon"), new HashSet<>(result.getAddresses()));
	}
	
	private static class Vehicle {
		
		private String name;
		
		private String color;
		
		private Vehicle(String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
		
		public String getColor() {
			return color;
		}
		
		public void setColor(String color) {
			this.color = color;
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
		
		public void addAddress(String address) {
			this.addresses.add(address);
		}
	}
}