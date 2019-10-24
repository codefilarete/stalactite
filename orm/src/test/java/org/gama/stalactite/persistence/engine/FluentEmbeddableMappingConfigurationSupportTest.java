package org.gama.stalactite.persistence.engine;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.reflection.AccessorChain;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.binder.LambdaParameterBinder;
import org.gama.stalactite.sql.binder.NullAwareParameterBinder;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.gama.stalactite.sql.result.Row;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupportInheritanceTest.Car;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupportInheritanceTest.Color;
import org.gama.stalactite.persistence.engine.IFluentEmbeddableMappingBuilder.IFluentEmbeddableMappingBuilderEmbedOptions;
import org.gama.stalactite.persistence.engine.IFluentEmbeddableMappingBuilder.IFluentEmbeddableMappingBuilderEmbeddableOptions;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Gender;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.engine.model.PersonWithGender;
import org.gama.stalactite.persistence.engine.model.Timestamp;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.gama.lang.collection.Iterables.collect;
import static org.gama.stalactite.sql.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
class FluentEmbeddableMappingConfigurationSupportTest {
	
	// NB: dialect is made non static because we register binder for the same column several times in these tests
	// and this is not supported : the very first one takes priority  
	private HSQLDBDialect dialect = new HSQLDBDialect();
	
	@BeforeEach
	public void initTest() {
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register((Class) Identifier.class,
				Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		dialect.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		dialect.getJavaTypeToSqlTypeMapping().put(Color.class, "int");
	}
	
	@Test
	void add_withoutName_targetedPropertyNameIsTaken() {
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table> mappingStrategy = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.add(Country::setDescription)
				.build(dialect, countryTable);
		
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
	void add_withoutName_withNamingStrategy_namingStrategyIsTaken_exceptIfColumnNameIsOverriden() {
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table> mappingStrategy = MappingEase.embeddableBuilder(Country.class)
				.columnNamingStrategy(accessor -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_col")
				.add(Country::getName)
				.add(Country::getDescription, "descriptionColumn")
				.build(dialect, countryTable);
		
		// column should be correctly created
		assertEquals(countryTable, mappingStrategy.getTargetTable());
		Column nameColumn = (Column) mappingStrategy.getTargetTable().mapColumnsOnName().get("name_col");
		assertNotNull(nameColumn);
		assertEquals(String.class, nameColumn.getJavaType());
		
		
		Column descriptionColumn = (Column) mappingStrategy.getTargetTable().mapColumnsOnName().get("descriptionColumn");
		assertNotNull(descriptionColumn);
		assertEquals(String.class, descriptionColumn.getJavaType());
	}
	
	@Test
	void add_withOverwrittenName_overwrittenNameIsTaken() {
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table> mappingStrategy = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName, "code")
				.add(Country::setDescription, "desc")
				.build(dialect, countryTable);
		
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
	void add_mappingDefinedTwiceByMethod_throwsException() {
		Table<?> countryTable = new Table<>("countryTable");
		assertEquals("Column name of mapping Country::getName is already targetted by o.g.s.p.e.m.Country.getName()",
				assertThrows(MappingConfigurationException.class, () -> MappingEase.embeddableBuilder(Country.class)
						.add(Country::getName)
						.add(Country::setName)
						.build(dialect, countryTable))
						.getMessage());
	}
	
	@Test
	void add_mappingDefinedTwiceByColumn_throwsException() {
		Table<?> countryTable = new Table<>("countryTable");
		assertEquals("Column xyz of mapping Country::getName is already targetted by o.g.s.p.e.m.Country.getDescription()",
				assertThrows(MappingConfigurationException.class, () -> MappingEase.embeddableBuilder(Country.class)
						.add(Country::getName, "xyz")
						.add(Country::setDescription, "xyz")
						.build(dialect, countryTable))
						.getMessage());
	}
	
	@Test
	void add_mappingAComplextType_throwsException() {
		Table<?> countryTable = new Table<>("countryTable");
		assertEquals("countryTable.timestamp has no matching binder, please consider adding one to dialect binder registry" +
						" or use one of the IFluentEmbeddableMappingConfiguration::embed methods",
				assertThrows(MappingConfigurationException.class, () -> MappingEase.embeddableBuilder(Country.class)
						.add(Country::getName)
						.add(Country::setTimestamp)
						.build(dialect, countryTable))
						.getMessage());
	}
	
	@Test
	void add_mandatory() {
		Table<?> countryTable = new Table<>("countryTable");
		MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName).mandatory()
				.add(Country::setDescription)
				.build(dialect, countryTable);
		
		// mandatory property sets column as mandatory
		assertFalse(countryTable.mapColumnsOnName().get("name").isNullable());
	}
	
	@Test
	void testEmbed_definedByGetter() {
		Table<?> countryTable = new Table<>("countryTable");
		MappingEase.embeddableBuilder(Person.class)
				.embed(Person::getTimestamp)
				.build(dialect, countryTable);
		
		Column creationDateColumn = countryTable.mapColumnsOnName().get("creationDate");
		assertNotNull(creationDateColumn);
		assertEquals(Date.class, creationDateColumn.getJavaType());
	}
	
	@Test
	void testEmbed_definedBySetter() {
		Table<?> countryTable = new Table<>("countryTable");
		MappingEase.embeddableBuilder(Person.class)
				.embed(Person::setTimestamp)
				.build(dialect, countryTable);
		
		Column creationDateColumn = countryTable.mapColumnsOnName().get("creationDate");
		assertNotNull(creationDateColumn);
		assertEquals(Date.class, creationDateColumn.getJavaType());
	}
	
	@Test
	void testEmbed_insertAndSelect_withOverridenColumnName() {
		Table<?> personTable = new Table<>("personTable");
		EmbeddedBeanMappingStrategy<Person, Table<?>> personMappingStrategy = MappingEase.embeddableBuilder(Person.class)
				.add(Person::getName)
				.embed(Person::setTimestamp)
					.overrideName(Timestamp::getCreationDate, "createdAt")
					.overrideName(Timestamp::getModificationDate, "modifiedAt")
				.build(dialect, personTable);
		
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
	void testInheritance_parentColumnsMustBeAdded() {
		EmbeddedBeanMappingStrategy<Car, Table> carMappingStrategy = MappingEase.embeddableBuilder(Car.class)
				// color is on a super class
				.add(Car::getColor)
				.add(Car::getModel)
				.build(dialect, new Table<>("car"));
		
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
	void testEmbed_inheritance_embeddedParentColumnsMustBeAdded() {
		EmbeddedBeanMappingStrategy<Car, Table> carMappingStrategy = MappingEase.embeddableBuilder(Car.class)
				.add(Car::getColor)
				.add(Car::getModel)
				// timestamp is on a super class
				.embed(Car::setTimestamp)
					.overrideName(Timestamp::getCreationDate, "createdAt")
					.overrideName(Timestamp::getModificationDate, "modifiedAt")
				.build(dialect, new Table<>("car"));
		
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
	
	@Test
	void testEmbed_insertAndSelect_withSomeExcludedProperty() {
		Table<?> personTable = new Table<>("personTable");
		EmbeddedBeanMappingStrategy<Person, Table<?>> personMappingStrategy = MappingEase.embeddableBuilder(Person.class)
				.add(Person::getName)
				.embed(Person::setTimestamp)
					.exclude(Timestamp::getCreationDate)
					.overrideName(Timestamp::getModificationDate, "modifiedAt")
				.build(dialect, personTable);
		
		Map<String, Column> columnsByName = (Map) personTable.mapColumnsOnName();
		
		// columns with getter name must be absent (hard to test: can be absent for many reasons !)
		assertNull(columnsByName.get("creationDate"));
		assertNull(columnsByName.get("modificationDate"));
		
		// Columns with good name must be present
		Column modifiedAtColumn = columnsByName.get("modifiedAt");
		assertNotNull(modifiedAtColumn);
		assertEquals(Date.class, modifiedAtColumn.getJavaType());
		
		Person person = new Person();
		person.setName("me");
		person.setTimestamp(new Timestamp());
		
		// insert value should contain embedded bean values (Timestamp)
		Map<Column<Table<?>, Object>, Object> insertValues = personMappingStrategy.getInsertValues(person);
		assertEquals(Maps
						.asHashMap(columnsByName.get("name"), (Object) person.getName())
						.add(modifiedAtColumn, person.getTimestamp().getModificationDate())
				, insertValues);
		
		Row row = new Row();
		row.put(modifiedAtColumn.getName(), person.getTimestamp().getCreationDate());
		
		Person loadedPerson = personMappingStrategy.transform(row);
		// we compare without with a round of 100ms because time laps between instanciation and database load
		assertEquals(person.getTimestamp().getCreationDate().getTime() / 100, loadedPerson.getTimestamp().getCreationDate().getTime() / 100);
	}
	
	@Test
	void testBuild_embed_definedTwice_throwException() {
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbedOptions<Country, Timestamp> mappingBuilder = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.embed(Country::getTimestamp)
				.embed(Country::getTimestamp);
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () -> mappingBuilder
				.build(dialect, countryTable));
		assertEquals("Country::getTimestamp is already mapped", thrownException.getMessage());
	}
	
	@Test
	void testBuild_embed() {
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbedOptions<Country, Timestamp> mappingBuilder = MappingEase.embeddableBuilder(Country.class)
				.columnNamingStrategy(accessor -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_col")
				.embed(Country::getTimestamp);
		EmbeddedBeanMappingStrategy<Country, ? extends Table<?>> mappingStrategy = mappingBuilder.build(dialect, countryTable);
		
		Map<String, ? extends Column<?, Object>> columnsByName = mappingStrategy.getTargetTable().mapColumnsOnName();
		assertNotNull(columnsByName.get("creationDate_col"));
		assertNotNull(columnsByName.get("modificationDate_col"));
	}
	
	@Test
	void testBuild_innerEmbed_withTwiceSameInnerEmbeddableName_throwException() {
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbedOptions<Country, Timestamp> mappingBuilder = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident)
					.exclude(Person::getCountry)
					.overrideName(Person::getName, "presidentName")
					.innerEmbed(Person::getTimestamp)
				// this embed will conflict with Country one because its type is already mapped with no override
				.embed(Country::getTimestamp);
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () -> mappingBuilder
				.build(dialect, countryTable));
		assertEquals("Country::getTimestamp conflicts with Person::getTimestamp while embedding a o.g.s.p.e.m.Timestamp" +
				", column names should be overriden : o.g.s.p.e.m.Timestamp.getCreationDate(), o.g.s.p.e.m.Timestamp.getModificationDate()",
				thrownException.getMessage());
		
		// we add an override, exception must still be thrown, with different message
		mappingBuilder.overrideName(Timestamp::getModificationDate, "modifiedAt");
		
		thrownException = assertThrows(MappingConfigurationException.class, () -> mappingBuilder
				.build(dialect, countryTable));
		assertEquals("Country::getTimestamp conflicts with Person::getTimestamp while embedding a o.g.s.p.e.m.Timestamp" +
				", column names should be overriden : o.g.s.p.e.m.Timestamp.getCreationDate()", thrownException.getMessage());
		
		// we override the last field, no exception is thrown
		mappingBuilder.overrideName(Timestamp::getCreationDate, "createdAt");
		mappingBuilder.build(dialect, countryTable);
		
		assertEquals(Arrays.asHashSet(
				// from Country
				"name",
				// from Person
				"id", "version", "presidentName", "creationDate", "modificationDate",
				// from Country.timestamp
				"createdAt", "modifiedAt"),
				collect(countryTable.getColumns(), Column::getName, HashSet::new));
	}
	
	@Test
	void testBuild_innerEmbed_withOverridenColumnName() {
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table<?>> personMappingStrategy = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident)
					.exclude(Person::getCountry)
					.overrideName(Person::getName, "presidentName")
					.innerEmbed(Person::getTimestamp)
						.overrideName(Timestamp::getCreationDate, "createdAt")
				.build(dialect, countryTable);
		
		Map<String, Column> columnsByName = (Map) personMappingStrategy.getTargetTable().mapColumnsOnName();
		
		assertEquals(Arrays.asHashSet(
				// from Country
				"name",
				// from Person
				"id", "presidentName", "version",
				// from Person.timestamp
				"createdAt", "modificationDate"),
				collect(countryTable.getColumns(), Column::getName, HashSet::new));
		
		// checking types
		assertEquals(Date.class, columnsByName.get("modificationDate").getJavaType());
		assertEquals(Date.class, columnsByName.get("createdAt").getJavaType());
		assertEquals(String.class, columnsByName.get("presidentName").getJavaType());
	}
	
	@Test
	void testBuild_withTwiceSameEmbeddableNames_throwsException() {
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbedOptions<Country, Person> mappingBuilder = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident);
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class,
				() -> mappingBuilder.build(dialect, countryTable));
		assertEquals("Error while mapping Country::getPresident : o.g.s.p.e.m.Person.name" +
				" conflicts with Country::getName because they use same column," +
				" override one of their name to avoid the conflict, see EmbedOptions::overrideName", thrownException.getMessage());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable_simpleCase() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
				.add(Person::getId);
		
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table<?>> personMappingStrategy = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName, "countryName")
				.embed(Country::getPresident, personMappingBuilder)
				.build(dialect, countryTable);
		
		Map<String, Column> columnsByName = (Map) personMappingStrategy.getTargetTable().mapColumnsOnName();
		
		assertEquals(Arrays.asHashSet(
				// from Country
				"countryName",
				// from Person
				"id"),
				countryTable.getColumns().stream().map(Column::getName).collect(Collectors.toSet()));
		
		// checking types
		assertEquals(String.class, columnsByName.get("countryName").getJavaType());
		assertEquals(Identifier.class, columnsByName.get("id").getJavaType());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable_simpleCase_setter() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
				.add(Person::getId);
		
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table<?>> personMappingStrategy = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName, "countryName")
				.embed(Country::setPresident, personMappingBuilder)
				.build(dialect, countryTable);
		
		Map<String, Column> columnsByName = (Map) personMappingStrategy.getTargetTable().mapColumnsOnName();
		
		assertEquals(Arrays.asHashSet(
				// from Country
				"countryName",
				// from Person
				"id"),
				countryTable.getColumns().stream().map(Column::getName).collect(Collectors.toSet()));
		
		// checking types
		assertEquals(String.class, columnsByName.get("countryName").getJavaType());
		assertEquals(Identifier.class, columnsByName.get("id").getJavaType());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable_overrideName() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
				.add(Person::getName)
				.embed(Person::getTimestamp);
		
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table<?>> personMappingStrategy = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident, personMappingBuilder)
					.overrideName(Person::getName, "personName")
				.build(dialect, countryTable);
		
		Map<String, Column> columnsByName = (Map) personMappingStrategy.getTargetTable().mapColumnsOnName();
		
		assertEquals(Arrays.asHashSet(
				// from Country
				"name",
				// from Person
				"personName", "modificationDate", "creationDate"),
				countryTable.getColumns().stream().map(Column::getName).collect(Collectors.toSet()));
		
		// checking types
		assertEquals(String.class, columnsByName.get("name").getJavaType());
		assertEquals(String.class, columnsByName.get("personName").getJavaType());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable_definedTwice_throwException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
				.add(Person::getName, "myName");
		
		Table<?> countryTable = new Table<>("countryTable");
		
		IFluentEmbeddableMappingBuilder<Country> mappingBuilder = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident, personMappingBuilder)
				// voluntary duplicate to fulfill goal of this test
				.embed(Country::getPresident, personMappingBuilder);
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				mappingBuilder.build(dialect, countryTable));
		assertEquals("Country::getPresident is already mapped", thrownException.getMessage());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable_conflictingColumnNameNotOverriden_throwsException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
				.add(Person::getName);
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<Country, Person> entityMappingBuilder = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident, personMappingBuilder);
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				entityMappingBuilder.build(dialect, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator() 
						+ "Embeddable definition 'Person::getName'" 
						+ " vs entity definition 'Country::getName' on column name 'name'",
				thrownException.getMessage());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable_columnNameOverridenOnConflictingName1_throwsException() {
		// Overriden vs override
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
				.add(Person::getName, "myName");
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<Country, Person> entityMappingBuilder = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName, "myName")
				.embed(Country::getPresident, personMappingBuilder);
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				entityMappingBuilder.build(dialect, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator() 
						+ "Embeddable definition 'Person::getName'" 
						+ " vs entity definition 'Country::getName' on column name 'myName'",
				thrownException.getMessage());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable_columnNameOverridenOnConflictingName2_throwsException() {
		// Overriden vs standard name
		EmbeddedBeanMappingStrategyBuilder<MyPerson> personMappingBuilder = MappingEase.embeddableBuilder(MyPerson.class)
				.add(MyPerson::getName, "myName");
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<MyCountry, MyPerson> entityMappingBuilder = MappingEase.embeddableBuilder(MyCountry.class)
				.add(MyCountry::getMyName)
				.embed(MyCountry::getPresident, personMappingBuilder);
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				entityMappingBuilder.build(dialect, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator()
						+ "Embeddable definition 'Person::getName'" 
						+ " vs entity definition 'MyCountry::getMyName' on column name 'myName'",
				thrownException.getMessage());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable_columnNameOverridenOnConflictingName3_throwsException() {
		// Standard name vs override
		EmbeddedBeanMappingStrategyBuilder<MyPerson> personMappingBuilder = MappingEase.embeddableBuilder(MyPerson.class)
				.add(MyPerson::getMyName);
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<MyCountry, MyPerson> entityMappingBuilder = MappingEase.embeddableBuilder(MyCountry.class)
				.add(Country::getName, "myName")
				.embed(MyCountry::getPresident, personMappingBuilder);
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				entityMappingBuilder.build(dialect, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator() 
						+ "Embeddable definition 'MyPerson::getMyName' vs entity definition" 
						+ " 'Country::getName' on column name 'myName'",
				thrownException.getMessage());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable_overrideNameOfUnmappedProperty_throwsException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
				.add(Person::getName);
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<Country, Person> entityMappingBuilder =
				MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident, personMappingBuilder)
					.overrideName(Person::getName, "personName")
					.overrideName(Person::getVersion, "personVersion");
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				entityMappingBuilder.build(dialect, countryTable));
		assertEquals("Person::getVersion is not mapped by embeddable strategy, so its column name override 'personVersion' can't apply",
				thrownException.getMessage());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable_embeddableContainsAnEmbeddedType() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
				.add(Person::getName, "personName")
				.embed(Person::getTimestamp)
					.overrideName(Timestamp::getCreationDate, "personCreatedAt")
					.overrideName(Timestamp::getModificationDate, "personModifiedAt");
		
		
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table<?>> personMappingStrategy = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.embed(Country::getTimestamp)
					.overrideName(Timestamp::getCreationDate, "createdAt")
					.overrideName(Timestamp::getModificationDate, "modifiedAt")
				.embed(Country::getPresident, personMappingBuilder)
					.overrideName(Person::getName, "presidentName")
				.build(dialect, countryTable);
		
		Map<String, Column> columnsByName = (Map) personMappingStrategy.getTargetTable().mapColumnsOnName();
		
		assertEquals(Arrays.asHashSet(
				// from Country
				"name", "createdAt", "modifiedAt",
				// from Person
				"presidentName", "personCreatedAt", "personModifiedAt"),
				countryTable.getColumns().stream().map(Column::getName).collect(Collectors.toSet()));
		
		// checking types
		assertEquals(String.class, columnsByName.get("name").getJavaType());
		assertEquals(Date.class, columnsByName.get("createdAt").getJavaType());
		assertEquals(Date.class, columnsByName.get("modifiedAt").getJavaType());
		assertEquals(String.class, columnsByName.get("presidentName").getJavaType());
		assertEquals(Date.class, columnsByName.get("personCreatedAt").getJavaType());
		assertEquals(Date.class, columnsByName.get("personModifiedAt").getJavaType());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable_embeddableContainsAnEmbeddedType2() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
				.add(Person::getName, "personName")
				.embed(Person::getTimestamp)
					.overrideName(Timestamp::getCreationDate, "personCreatedAt")
					.exclude(Timestamp::getModificationDate);
		
		
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table<?>> personMappingStrategy = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.embed(Country::getTimestamp)
					.overrideName(Timestamp::getCreationDate, "createdAt")
					.overrideName(Timestamp::getModificationDate, "modifiedAt")
				.embed(Country::getPresident, personMappingBuilder)
					.overrideName(Person::getName, "presidentName")
					.overrideName(AccessorChain.chain(Person::getTimestamp, Timestamp::getCreationDate), "presidentElectedAt")
				.build(dialect, countryTable);
		
		Map<String, Column> columnsByName = (Map) personMappingStrategy.getTargetTable().mapColumnsOnName();
		
		assertEquals(Arrays.asHashSet(
				// from Country
				"name", "createdAt", "modifiedAt",
				// from Person
				"presidentName", "presidentElectedAt"),
				countryTable.getColumns().stream().map(Column::getName).collect(Collectors.toSet()));
		
		// checking types
		assertEquals(String.class, columnsByName.get("name").getJavaType());
		assertEquals(Date.class, columnsByName.get("createdAt").getJavaType());
		assertEquals(Date.class, columnsByName.get("modifiedAt").getJavaType());
		assertEquals(String.class, columnsByName.get("presidentName").getJavaType());
		assertEquals(Date.class, columnsByName.get("presidentElectedAt").getJavaType());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable_embeddableContainsAnEmbeddedType_throwsException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
				.embed(Person::getTimestamp);
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<Country, Person> mappingBuilder = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.embed(Country::getTimestamp)
				.embed(Country::getPresident, personMappingBuilder);
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				mappingBuilder.build(dialect, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator()
				+ "Embeddable definition 'Person::getTimestamp > o.g.s.p.e.m.Timestamp.getModificationDate()'"
				+ " vs entity definition 'Country::getTimestamp > o.g.s.p.e.m.Timestamp.getModificationDate()' on column name 'modificationDate'"
				+ System.lineSeparator()
				+ "Embeddable definition 'Person::getTimestamp > o.g.s.p.e.m.Timestamp.getCreationDate()'"
				+ " vs entity definition 'Country::getTimestamp > o.g.s.p.e.m.Timestamp.getCreationDate()' on column name 'creationDate'",
				thrownException.getMessage());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable_embeddableContainsAnEmbeddedType_withOverridenName_throwsException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
				.embed(Person::getTimestamp)
				.overrideName(Timestamp::getCreationDate, "createdAt");
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<Country, Person> mappingBuilder = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.embed(Country::getTimestamp)
				.embed(Country::getPresident, personMappingBuilder);
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				mappingBuilder.build(dialect, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator()
				+ "Embeddable definition 'Person::getTimestamp > o.g.s.p.e.m.Timestamp.getModificationDate()'"
				+ " vs entity definition 'Country::getTimestamp > o.g.s.p.e.m.Timestamp.getModificationDate()' on column name 'modificationDate'",
				thrownException.getMessage());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable_embeddableContainsAnEmbeddedType_withExclusion_throwsException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
				.embed(Person::getTimestamp)
				.exclude(Timestamp::getCreationDate);
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<Country, Person> mappingBuilder = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.embed(Country::getTimestamp)
				.embed(Country::getPresident, personMappingBuilder);
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				mappingBuilder.build(dialect, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator()
				+ "Embeddable definition 'Person::getTimestamp > o.g.s.p.e.m.Timestamp.getModificationDate()'"
				+ " vs entity definition 'Country::getTimestamp > o.g.s.p.e.m.Timestamp.getModificationDate()' on column name 'modificationDate'",
				thrownException.getMessage());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable_embeddableContainsAnEmbeddedType_andIsOverriden_throwsException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
				.embed(Person::getTimestamp);
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilder<Country> mappingBuilder = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.embed(Country::getTimestamp)
				.embed(Country::getPresident, personMappingBuilder)
					.overrideName(AccessorChain.chain(Person::getTimestamp, Timestamp::getCreationDate), "createdAt");
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				mappingBuilder.build(dialect, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator()
				+ "Embeddable definition 'Person::getTimestamp > o.g.s.p.e.m.Timestamp.getModificationDate()'"
				+ " vs entity definition 'Country::getTimestamp > o.g.s.p.e.m.Timestamp.getModificationDate()' on column name 'modificationDate'",
				thrownException.getMessage());
	}
	
	@Test
	void testBuild_embedReusedEmbeddable__embeddableContainsAnEmbeddedType_overrideNameOfUnmappedProperty_throwsException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
				.embed(Person::getTimestamp)
					.exclude(Timestamp::getModificationDate);
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilder<Country> mappingBuilder = MappingEase.embeddableBuilder(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident, personMappingBuilder)
					.overrideName(AccessorChain.chain(Person::getTimestamp, Timestamp::getModificationDate), "electedAt");
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				mappingBuilder.build(dialect, countryTable));
		assertEquals("Person::getTimestamp > Timestamp::getModificationDate is not mapped by embeddable strategy, so its column name override 'electedAt' can't apply",
				thrownException.getMessage());
	}
	
	@Test
	void addEnum() throws SQLException {
		Table<?> personTable = new Table<>("personTable");
		EmbeddedBeanMappingStrategy<PersonWithGender, Table> personMappingStrategy = MappingEase.embeddableBuilder(PersonWithGender.class)
				.add(Person::getName)
				.addEnum(PersonWithGender::getGender)
				.build(dialect, personTable);
		
		PersonWithGender person = new PersonWithGender();
		person.setName("toto");
		person.setGender(Gender.FEMALE);
		Map<Column<Table, Object>, Object> insertValues = personMappingStrategy.getInsertValues(person);
		assertEquals("toto", insertValues.get(personTable.mapColumnsOnName().get("name")));
		assertEquals(Gender.FEMALE, insertValues.get(personTable.mapColumnsOnName().get("gender")));
		
		ParameterBinder genderColumnBinder = dialect.getColumnBinderRegistry().getBinder(personTable.mapColumnsOnName().get("gender"));
		// by default, gender will be mapped on its name
		PreparedStatement mock = mock(PreparedStatement.class);
		genderColumnBinder.set(mock, 1, person.getGender());
		verify(mock).setString(1, "FEMALE");
	}
		
	@Test
		void addEnum_byOrdinal() throws SQLException {
		Table<?> personTable = new Table<>("personTable");
		EmbeddedBeanMappingStrategy<PersonWithGender, Table> personMappingStrategy = MappingEase.embeddableBuilder(PersonWithGender.class)
				.add(Person::getName)
				.addEnum(PersonWithGender::getGender).byOrdinal()
				.build(dialect, personTable);
		
		PersonWithGender person = new PersonWithGender();
		person.setName("toto");
		person.setGender(Gender.FEMALE);
		Map<Column<Table, Object>, Object> insertValues = personMappingStrategy.getInsertValues(person);
		assertEquals("toto", insertValues.get(personTable.mapColumnsOnName().get("name")));
		assertEquals(Gender.FEMALE, insertValues.get(personTable.mapColumnsOnName().get("gender")));
		
		ParameterBinder genderColumnBinder = dialect.getColumnBinderRegistry().getBinder(personTable.mapColumnsOnName().get("gender"));
		// checking that binder send ordinal value to JDBC
		PreparedStatement mock = mock(PreparedStatement.class);
		genderColumnBinder.set(mock, 1, person.getGender());
		verify(mock).setInt(1, 1);
	}
	
	@Test
	void addEnum_byName() throws SQLException {
		Table<?> personTable = new Table<>("personTable");
		EmbeddedBeanMappingStrategy<PersonWithGender, Table> personMappingStrategy = MappingEase.embeddableBuilder(PersonWithGender.class)
				.add(Person::getName)
				.addEnum(PersonWithGender::getGender).byName()
				.build(dialect, personTable);
		
		PersonWithGender person = new PersonWithGender();
		person.setName("toto");
		person.setGender(Gender.FEMALE);
		Map<Column<Table, Object>, Object> insertValues = personMappingStrategy.getInsertValues(person);
		assertEquals("toto", insertValues.get(personTable.mapColumnsOnName().get("name")));
		assertEquals(Gender.FEMALE, insertValues.get(personTable.mapColumnsOnName().get("gender")));
		
		ParameterBinder genderColumnBinder = dialect.getColumnBinderRegistry().getBinder(personTable.mapColumnsOnName().get("gender"));
		// checking that binder send name to JDBC
		PreparedStatement mock = mock(PreparedStatement.class);
		genderColumnBinder.set(mock, 1, person.getGender());
		verify(mock).setString(1, "FEMALE");
	}
	
	@Test
	void addEnum_mandatory() {
		Table<?> personTable = new Table<>("personTable");
		MappingEase.embeddableBuilder(PersonWithGender.class)
				.add(Person::getName)
				.addEnum(PersonWithGender::getGender).mandatory()
				.build(dialect, personTable);
		
		// mandatory property sets column as mandatory
		assertFalse(personTable.mapColumnsOnName().get("gender").isNullable());
	}
	
	/**
	 * Test to check that the API returns right Object which means:
	 * - interfaces are well written to return right types, so one can chain others methods
	 * - at runtime instance of the right type is also returned
	 * (avoid "java.lang.ClassCastException: com.sun.proxy.$Proxy10 cannot be cast to org.gama.stalactite.persistence.engine
	 * .IFluentEmbeddableMappingBuilder")
	 * <p>
	 * As many as possible combinations of method chaining should be done here, because all combination seems impossible, this test must be
	 * considered
	 * as a best effort, and any regression found in user code should be added here
	 */
	@Test
	void apiUsage() {
		Table<?> countryTable = new Table<>("countryTable");
		
		try {
			MappingEase.embeddableBuilder(Country.class)
					.add(Country::getName)
					.embed(Country::getPresident)
					.overrideName(Person::getId, "personId")
					.overrideName(Person::getName, "personName")
					.innerEmbed(Person::getTimestamp)
					.embed(Country::getTimestamp)
					.add(Country::getId)
					.add(Country::setDescription, "zxx")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					.build(dialect, countryTable);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			MappingEase.embeddableBuilder(Country.class)
					.add(Country::getName)
					.embed(Country::getPresident)
					.innerEmbed(Person::getTimestamp)
					.embed(Country::getTimestamp)
					.add(Country::getId, "zz")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					.add(Country::getDescription, "xx")
					.build(dialect, countryTable);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			MappingEase.embeddableBuilder(Country.class)
					.add(Country::getName)
					.add(Country::getId, "zz")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					// embed with setter
					.embed(Country::setPresident)
					// inner embed with setter
					.innerEmbed(Person::setTimestamp)
					// embed with setter
					.embed(Country::setTimestamp)
					.add(Country::getDescription, "xx")
					.add(Country::getDummyProperty, "dd")
					.build(dialect, countryTable);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
					.add(Person::getName);
			
			MappingEase.embeddableBuilder(Country.class)
					.add(Country::getName)
					.add(Country::getId, "zz")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					// embed with setter
					.embed(Country::getPresident, personMappingBuilder)
					.build(dialect, countryTable);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
					.add(Person::getName);
			
			MappingEase.embeddableBuilder(Country.class)
					.add(Country::getName)
					.add(Country::getId, "zz")
					.embed(Country::getPresident, personMappingBuilder)
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					// reusing embeddable ...
					.embed(Country::getPresident, personMappingBuilder)
					// with getter override
					.overrideName(Person::getName, "toto")
					// with setter override
					.overrideName(Person::setName, "tata")
					.build(dialect, countryTable);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			MappingEase.embeddableBuilder(PersonWithGender.class)
					.add(Person::getName)
					.addEnum(PersonWithGender::getGender).byOrdinal()
					.embed(Person::setTimestamp)
					.overrideName(Timestamp::getCreationDate, "myDate")
					.addEnum(PersonWithGender::getGender, "MM").byOrdinal()
					.add(PersonWithGender::getId, "zz")
					.addEnum(PersonWithGender::setGender).byName()
					.embed(Person::getTimestamp)
					.addEnum(PersonWithGender::setGender, "MM").byName()
					.build(dialect, new Table<>("person"));
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
	}
	
	static class MyPerson extends Person {
		private String myName;
		
		public String getMyName() {
			return myName;
		}
	}
	
	static class MyCountry extends Country {
		private String myName;
		
		public String getMyName() {
			return myName;
		}
		
		@Override
		public MyPerson getPresident() {
			return (MyPerson) super.getPresident();
		}
	}
}