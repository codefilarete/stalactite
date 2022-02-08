package org.gama.stalactite.sql.result;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.danekja.java.util.function.serializable.SerializableFunction;
import org.danekja.java.util.function.serializable.SerializableSupplier;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.trace.ModifiableInt;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.gama.stalactite.sql.binder.DefaultResultSetReaders.INTEGER_READER;
import static org.gama.stalactite.sql.binder.DefaultResultSetReaders.STRING_READER;

/**
 * @author Guillaume Mary
 */
class ResultSetRowTransformerTest {
	
	@Test
	void transform_basicUseCase() throws SQLException {
		// Binding column "vehicleType" to the constructor of Car
		ResultSetRowTransformer<String, Car> testInstance = new ResultSetRowTransformer<>(Car.class, "vehicleType", STRING_READER, Car::new);
		// Adding a column to fill the bean
		testInstance.add("wheels", INTEGER_READER, Car::setWheelCount);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class).add("vehicleType", "bicycle").add("wheels", 2),
				Maps.forHashMap(String.class, Object.class).add("vehicleType", "moto").add("wheels", 2),
				Maps.forHashMap(String.class, Object.class).add("vehicleType", "car").add("wheels", 4),
				Maps.forHashMap(String.class, Object.class).add("vehicleType", "car").add("wheels", 6)
		));
		
		resultSet.next();
		Car vehicle1 = testInstance.transform(resultSet);
		assertThat(vehicle1.getName()).isEqualTo("bicycle");
		assertThat(vehicle1.getWheelCount()).isEqualTo(2);
		resultSet.next();
		Car vehicle2 = testInstance.transform(resultSet);
		assertThat(vehicle2.getName()).isEqualTo("moto");
		assertThat(vehicle2.getWheelCount()).isEqualTo(2);
		resultSet.next();
		Car vehicle3 = testInstance.transform(resultSet);
		assertThat(vehicle3.getName()).isEqualTo("car");
		assertThat(vehicle3.getWheelCount()).isEqualTo(4);
		resultSet.next();
		Car vehicle4 = testInstance.transform(resultSet);
		assertThat(vehicle4.getName()).isEqualTo("car");
		// vehicle3 and vehicle4 shouldn't be the same despite their identical name
		assertThat(vehicle3).isNotSameAs(vehicle4);
	}
	
	@Test
	void transform_noArgConstructor_basicUseCase() throws SQLException {
		// Binding column "vehicleType" to the constructor of Car
		ResultSetRowTransformer<String, Car> testInstance = new ResultSetRowTransformer<>(Car.class, Car::new);
		// Adding a column to fill the bean
		testInstance.add("wheels", INTEGER_READER, Car::setWheelCount);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
			Maps.forHashMap(String.class, Object.class).add("vehicleType", "bicycle").add("wheels", 2),
			Maps.forHashMap(String.class, Object.class).add("vehicleType", "moto").add("wheels", 2),
			Maps.forHashMap(String.class, Object.class).add("vehicleType", "car").add("wheels", 4),
			Maps.forHashMap(String.class, Object.class).add("vehicleType", "car").add("wheels", 6)
		));
		
		resultSet.next();
		Car vehicle1 = testInstance.transform(resultSet);
		assertThat(vehicle1.getName()).isNull();
		assertThat(vehicle1.getWheelCount()).isEqualTo(2);
		resultSet.next();
		Car vehicle2 = testInstance.transform(resultSet);
		assertThat(vehicle2.getName()).isNull();
		assertThat(vehicle2.getWheelCount()).isEqualTo(2);
		resultSet.next();
		Car vehicle3 = testInstance.transform(resultSet);
		assertThat(vehicle3.getName()).isNull();
		assertThat(vehicle3.getWheelCount()).isEqualTo(4);
		resultSet.next();
		Car vehicle4 = testInstance.transform(resultSet);
		assertThat(vehicle4.getName()).isNull();
		// vehicle3 and vehicle4 shouldn't be the same despite their identical name
		assertThat(vehicle3).isNotSameAs(vehicle4);
	}
	
	@Test
	void transform_basicUseCase_complexConsumer() throws SQLException {
		// The default ModifiableInt that takes its value from "a". Reinstanciated on each row.
		ResultSetRowTransformer<Integer, ModifiableInt> testInstance = new ResultSetRowTransformer<>(ModifiableInt.class, "a", INTEGER_READER, ModifiableInt::new);
		// The secondary that will increment the same ModifiableInt by column "b" value
		testInstance.add(new ColumnConsumer<>("b", INTEGER_READER, (t, i) -> t.increment(Objects.preventNull(i, 0))));
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class).add("a", 42).add("b", 1),
				Maps.forHashMap(String.class, Object.class).add("a", 666).add("b", null)
		));
		
		resultSet.next();
		assertThat(testInstance.transform(resultSet).getValue()).isEqualTo(43);
		resultSet.next();
		// no change on this one because "b" column is null on the row and we took null into account during incrementation
		assertThat(testInstance.transform(resultSet).getValue()).isEqualTo(666);
	}
	
	/**
	 * A test based on an {@link ModifiableInt} that would take its value from a {@link java.sql.ResultSet}
	 */
	@Test
	void transform_shareInstanceOverRows() throws SQLException {
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
		assertThat(testInstance.transform(resultSet).getValue()).isEqualTo(43);
		resultSet.next();
		// no change on this one because "b" column is null on the row and we took null into account during incrementation
		assertThat(testInstance.transform(resultSet).getValue()).isEqualTo(709);
	}
	
	
	@Test
	void copyWithAliases() throws SQLException {
		ResultSetRowTransformer<Integer, ModifiableInt> sourceInstance = new ResultSetRowTransformer<>(ModifiableInt.class, "a", INTEGER_READER, ModifiableInt::new);
		sourceInstance.add(new ColumnConsumer<>("b", INTEGER_READER, (t, i) -> t.increment(Objects.preventNull(i, 0))));
		
		// we're making our copy with column "a" is now "x", and column "b" is now "y"
		ResultSetRowTransformer<Integer, ModifiableInt> testInstance = sourceInstance.copyWithAliases(Maps.asHashMap("a", "x").add("b", "y"));
		
		// of course ....
		assertThat(testInstance).isNotSameAs(sourceInstance);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class).add("x", 42).add("y", 1),
				Maps.forHashMap(String.class, Object.class).add("x", 666).add("y", null)
		));
		
		resultSet.next();
		assertThat(testInstance.transform(resultSet).getValue()).isEqualTo(43);
		resultSet.next();
		// no change on this one because "b" column is null on the row and we took null into account during incrementation
		assertThat(testInstance.transform(resultSet).getValue()).isEqualTo(666);
	}
	
	@Test
	void copyFor() throws SQLException {
		ResultSetRowTransformer<String, Vehicle> sourceInstance = new ResultSetRowTransformer<>(Vehicle.class, "name", STRING_READER, Vehicle::new);
		sourceInstance.add(new ColumnConsumer<>("color", STRING_READER, Vehicle::setColor));
		
		ResultSetRowTransformer<String, Car> testInstance = sourceInstance.copyFor(Car.class, (SerializableFunction<String, Car>) Car::new);
		testInstance.add(new ColumnConsumer<>("wheels", INTEGER_READER, Car::setWheelCount));
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class).add("name", "peugeot").add("wheels", 4).add("color", "red")
		));
		
		resultSet.next();
		Car result = testInstance.transform(resultSet);
		assertThat(result.getName()).isEqualTo("peugeot");
		assertThat(result.getWheelCount()).isEqualTo(4);
		assertThat(result.getColor()).isEqualTo("red");
	}
	
	@Test
	void copyFor_withFunctionAsArg_instanceIsCreatedWithSupplier_throwsException() {
		ResultSetRowTransformer<String, Vehicle> sourceInstance = new ResultSetRowTransformer<>(Vehicle.class, Vehicle::new);
		sourceInstance.add(new ColumnConsumer<>("color", STRING_READER, Vehicle::setColor));
		
		assertThatThrownBy(() -> sourceInstance.copyFor(Car.class, (SerializableFunction<String, Car>) Car::new))
			.isInstanceOf(UnsupportedOperationException.class)
			.hasMessage("This instance can only be cloned with an identifier-arg constructor because it was created with one");
	}
	
	@Test
	void copyFor_withSupplierAsArg_instanceIsCreatedWithFunction_throwsException() {
		ResultSetRowTransformer<String, Vehicle> sourceInstance = new ResultSetRowTransformer<>(Vehicle.class, "name", STRING_READER, Vehicle::new);
		sourceInstance.add(new ColumnConsumer<>("color", STRING_READER, Vehicle::setColor));
		
		assertThatThrownBy(() -> sourceInstance.copyFor(Car.class, (SerializableSupplier<Car>) Car::new))
			.isInstanceOf(UnsupportedOperationException.class)
			.hasMessage("This instance can only be cloned with a no-arg constructor because it was created with one");
	}
	
	@Test
	void exampleWithCollection() throws SQLException {
		ResultSetRowTransformer<String, Person> testInstance = new ResultSetRowTransformer<>(Person.class, "name", STRING_READER, Person::new);
		
		testInstance.add("address1", STRING_READER, Person::getAddresses, Person::setAddresses, ArrayList::new);
		testInstance.add("address2", STRING_READER, Person::getAddresses, Person::setAddresses, ArrayList::new);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, String.class).add("name", "paul").add("address1", "rue Vaugirard").add("address2", "rue Menon")
		));
		
		resultSet.next();
		Person result = testInstance.transform(resultSet);
		assertThat(result.getName()).isEqualTo("paul");
		assertThat(new HashSet<>(result.getAddresses())).isEqualTo(Arrays.asSet("rue Vaugirard", "rue Menon"));
	}
	
	@Test
	void relation() throws SQLException {
		ResultSetRowTransformer<String, Person> testInstance = new ResultSetRowTransformer<>(Person.class, "name", STRING_READER, Person::new);
		
		ResultSetRowTransformer<String, String> addressTransformer = new ResultSetRowTransformer<>(String.class, "address1", STRING_READER, SerializableFunction.identity());
		testInstance.add(Person::addAddress, addressTransformer);
		testInstance.add(Person::addAddress, addressTransformer.copyWithAliases(s -> "address2"));
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, String.class).add("name", "paul").add("address1", "rue Vaugirard").add("address2", "rue Menon")
		));
		
		resultSet.next();
		Person result = testInstance.transform(resultSet);
		assertThat(result.getName()).isEqualTo("paul");
		assertThat(new HashSet<>(result.getAddresses())).isEqualTo(Arrays.asSet("rue Vaugirard", "rue Menon"));
	}
	
	private static class Vehicle {
		
		private String name;
		
		private String color;
		
		private Vehicle(String name) {
			this.name = name;
		}
		
		private Vehicle() {
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
		
		private Car() {
			super();
		}
		
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