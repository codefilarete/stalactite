package org.gama.stalactite.persistence.engine;

import java.util.Date;
import java.util.Map;

import org.gama.lang.Reflections;
import org.gama.lang.collection.Maps;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.binder.LambdaParameterBinder;
import org.gama.sql.binder.NullAwareParameterBinder;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.engine.FluentMappingBuilderInheritanceTest.Car;
import org.gama.stalactite.persistence.engine.FluentMappingBuilderInheritanceTest.Color;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.engine.model.Timestamp;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.gama.sql.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Guillaume Mary
 */
class FluentEmbeddableMappingBuilderSupportTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	
	@BeforeAll
	public static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class,
				Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRGB)));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Color.class, "int");
	}
	
	@Test
	public void testAdd_withoutName_targetedPropertyNameIsTaken() {
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table> mappingStrategy = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.add(Country::setDescription)
				.build(DIALECT, countryTable);
		
		// column should be correctly created
		assertEquals(countryTable, mappingStrategy.getTargetTable());
		Column nameColumn = (Column) mappingStrategy.getTargetTable().mapColumnsOnName().get("name");
		assertNotNull(nameColumn);
		assertEquals(String.class, nameColumn.getJavaType());
		
		Column descriptionColumn = (Column) mappingStrategy.getTargetTable().mapColumnsOnName().get("description");
		assertNotNull(descriptionColumn);
		assertEquals(String.class, descriptionColumn.getJavaType());
	}
	
	@Test
	public void testAdd_withOverwrittenName_overwrittenNameIsTaken() {
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table> mappingStrategy = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName, "code")
				.add(Country::setDescription, "desc")
				.build(DIALECT, countryTable);
		
		// column should be correctly created
		assertEquals(countryTable, mappingStrategy.getTargetTable());
		Column nameColumn = (Column) mappingStrategy.getTargetTable().mapColumnsOnName().get("code");
		assertNotNull(nameColumn);
		assertEquals(String.class, nameColumn.getJavaType());
		
		Column descriptionColumn = (Column) mappingStrategy.getTargetTable().mapColumnsOnName().get("desc");
		assertNotNull(descriptionColumn);
		assertEquals(String.class, descriptionColumn.getJavaType());
	}
	
	@Test
	public void testAdd_mappingDefinedTwiceByMethod_throwsException() throws NoSuchMethodException {
		Table<?> countryTable = new Table<>("countryTable");
		assertEquals("Mapping is already defined by method " + Reflections.toString(Country.class.getMethod("getName")),
				assertThrows(IllegalArgumentException.class, () -> FluentEmbeddableMappingConfigurationSupport.from(Country.class)
						.add(Country::getName)
						.add(Country::setName)
						.build(DIALECT, countryTable))
						.getMessage());
	}
	
	@Test
	public void testAdd_mappingDefinedTwiceByColumn_throwsException() {
		Table<?> countryTable = new Table<>("countryTable");
		assertEquals("Mapping is already defined for column xyz",
				assertThrows(IllegalArgumentException.class, () -> FluentEmbeddableMappingConfigurationSupport.from(Country.class)
						.add(Country::getName, "xyz")
						.add(Country::setDescription, "xyz")
						.build(DIALECT, countryTable))
						.getMessage());
	}
	
	@Test
	public void testEmbed_definedByGetter() {
		Table<?> countryTable = new Table<>("countryTable");
		FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.embed(Person::getTimestamp)
				.build(DIALECT, countryTable);
		
		Column creationDateColumn = countryTable.mapColumnsOnName().get("creationDate");
		assertNotNull(creationDateColumn);
		assertEquals(Date.class, creationDateColumn.getJavaType());
	}
	
	@Test
	public void testEmbed_definedBySetter() {
		Table<?> countryTable = new Table<>("countryTable");
		FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.embed(Person::setTimestamp)
				.build(DIALECT, countryTable);
		
		Column creationDateColumn = countryTable.mapColumnsOnName().get("creationDate");
		assertNotNull(creationDateColumn);
		assertEquals(Date.class, creationDateColumn.getJavaType());
	}
	
	@Test
	public void testEmbed_insertAndSelect_withOverridenColumnName() {
		Table<?> personTable = new Table<>("personTable");
		EmbeddedBeanMappingStrategy<Person, Table<?>> personMappingStrategy = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.add(Person::getName)
				.embed(Person::setTimestamp)
				.overrideName(Timestamp::getCreationDate, "createdAt")
				.overrideName(Timestamp::getModificationDate, "modifiedAt")
				.build(DIALECT, personTable);
		
		Map<String, Column> columnsByName = (Map) personTable.mapColumnsOnName();
		
		// columns with getter name must be absent (hard to test: can be absent for many reasons !)
		assertNull(columnsByName.get("creationDate"));
		assertNull(columnsByName.get("modificationDate"));
		
		// Columns with good name must be present
		Column modifiedAtColumn = columnsByName.get("modifiedAt");
		assertNotNull(modifiedAtColumn);
		assertEquals(Date.class, modifiedAtColumn.getJavaType());
		Column createdAtColumn = columnsByName.get("createdAt");
		assertNotNull(createdAtColumn);
		assertEquals(Date.class, createdAtColumn.getJavaType());
		
		Person person = new Person();
		person.setName("me");
		person.setTimestamp(new Timestamp());
		
		// insert value should contain embedded bean values (Timestamp)
		Map<Column<Table<?>, Object>, Object> insertValues = personMappingStrategy.getInsertValues(person);
		assertEquals(Maps
						.asHashMap(columnsByName.get("name"), (Object) person.getName())
						.add(modifiedAtColumn, person.getTimestamp().getModificationDate())
						.add(createdAtColumn, person.getTimestamp().getCreationDate())
				, insertValues);
		
		Row row = new Row();
		row.put(createdAtColumn.getName(), person.getTimestamp().getCreationDate());
		
		Person loadedPerson = personMappingStrategy.transform(row);
		assertEquals(person.getTimestamp().getCreationDate(), loadedPerson.getTimestamp().getCreationDate());
	}
	
	@Test
	public void testInheritance_parentColumnsMustBeAdded() {
		EmbeddedBeanMappingStrategy<Car, Table> carMappingStrategy = FluentEmbeddableMappingConfigurationSupport.from(Car.class)
				// color is on a super class
				.add(Car::getColor)
				.add(Car::getModel)
				.build(DIALECT, new Table<>("car"));
		
		Map<String, Column> columnsByName = (Map) carMappingStrategy.getTargetTable().mapColumnsOnName();
		
		// Columns with good name must be present
		Column colorColumn = columnsByName.get("color");
		assertNotNull(colorColumn);
		assertEquals(Color.class, colorColumn.getJavaType());
		Column modelColumn = columnsByName.get("model");
		assertNotNull(modelColumn);
		assertEquals(String.class, modelColumn.getJavaType());
		
		Car car = new Car(42L);
		car.setModel("Renault");
		car.setColor(new Color(666));
		
		// insert values should contain Color one even if it's owned by a super class
		Map<Column<Table, Object>, Object> insertValues = carMappingStrategy.getInsertValues(car);
		assertEquals(Maps.asHashMap(colorColumn, (Object) new Color(666)).add(modelColumn, "Renault"), insertValues);
	}
	
	@Test
	public void testEmbed_inheritance_embeddedParentColumnsMustBeAdded() {
		EmbeddedBeanMappingStrategy<Car, Table> carMappingStrategy = FluentEmbeddableMappingConfigurationSupport.from(Car.class)
				.add(Car::getColor)
				.add(Car::getModel)
				// timestamp is on a super class
				.embed(Car::setTimestamp)
				.overrideName(Timestamp::getCreationDate, "createdAt")
				.overrideName(Timestamp::getModificationDate, "modifiedAt")
				.build(DIALECT, new Table<>("car"));
		
		Map<String, Column> columnsByName = (Map) carMappingStrategy.getTargetTable().mapColumnsOnName();
		
		// Columns with good name must be present
		// columns with getter name must be absent (hard to test: can be absent for many reasons !)
		assertNull(columnsByName.get("creationDate"));
		assertNull(columnsByName.get("modificationDate"));
		
		// Columns with good name must be present
		Column modifiedAtColumn = columnsByName.get("modifiedAt");
		assertNotNull(modifiedAtColumn);
		assertEquals(Date.class, modifiedAtColumn.getJavaType());
		Column createdAtColumn = columnsByName.get("createdAt");
		assertNotNull(createdAtColumn);
		assertEquals(Date.class, createdAtColumn.getJavaType());
		
		Column colorColumn = columnsByName.get("color");
		assertNotNull(colorColumn);
		assertEquals(Color.class, colorColumn.getJavaType());
		Column modelColumn = columnsByName.get("model");
		assertNotNull(modelColumn);
		assertEquals(String.class, modelColumn.getJavaType());
		
		Car car = new Car(42L);
		car.setModel("Renault");
		car.setColor(new Color(666));
		
		// insert values should contain null because car's timestamp is not initialized
		Map<Column<Table, Object>, Object> insertValues = carMappingStrategy.getInsertValues(car);
		assertEquals(Maps
						.asHashMap(colorColumn, (Object) car.getColor())
						.add(modelColumn, car.getModel())
						.add(modifiedAtColumn, null)
						.add(createdAtColumn, null)
				, insertValues);
		
		// insert values should contain car's Timestamp because it is initialized
		car.setTimestamp(new Timestamp());
		insertValues = carMappingStrategy.getInsertValues(car);
		assertEquals(Maps
						.asHashMap(colorColumn, (Object) car.getColor())
						.add(modelColumn, car.getModel())
						.add(modifiedAtColumn, car.getTimestamp().getModificationDate())
						.add(createdAtColumn, car.getTimestamp().getCreationDate())
				, insertValues);
		
		// loaded Car should not contain Timestamp
		Row row = new Row();
		row.put(createdAtColumn.getName(), null);
		Car loadedCar = carMappingStrategy.transform(row);
		assertNull(loadedCar.getTimestamp());
		
		// loaded Car should contain persisted car's Timestamp
		row.put(createdAtColumn.getName(), car.getTimestamp().getCreationDate());
		loadedCar = carMappingStrategy.transform(row);
		assertEquals(car.getTimestamp().getCreationDate(), loadedCar.getTimestamp().getCreationDate());
	}
}