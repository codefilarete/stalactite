package org.gama.stalactite.persistence.engine;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.gama.lang.Reflections;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.reflection.AccessorChain;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.binder.LambdaParameterBinder;
import org.gama.sql.binder.NameEnumParameterBinder;
import org.gama.sql.binder.NullAwareParameterBinder;
import org.gama.sql.binder.OrdinalEnumParameterBinder;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.engine.FluentMappingBuilderInheritanceTest.Car;
import org.gama.stalactite.persistence.engine.FluentMappingBuilderInheritanceTest.Color;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.gama.sql.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Guillaume Mary
 */
class FluentEmbeddableMappingConfigurationSupportTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	
	@BeforeAll
	public static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class,
				Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
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
	public void testAdd_withoutName_withNamingStrategy_namingStrategyIsTaken_exceptIfColumnNameIsOverriden() {
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table> mappingStrategy = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.columnNamingStrategy(accessor -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_col")
				.add(Country::getName)
				.add(Country::getDescription, "descriptionColumn")
				.build(DIALECT, countryTable);
		
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
				assertThrows(MappingConfigurationException.class, () -> FluentEmbeddableMappingConfigurationSupport.from(Country.class)
						.add(Country::getName)
						.add(Country::setName)
						.build(DIALECT, countryTable))
						.getMessage());
	}
	
	@Test
	public void testAdd_mappingDefinedTwiceByColumn_throwsException() {
		Table<?> countryTable = new Table<>("countryTable");
		assertEquals("Mapping is already defined for column xyz",
				assertThrows(MappingConfigurationException.class, () -> FluentEmbeddableMappingConfigurationSupport.from(Country.class)
						.add(Country::getName, "xyz")
						.add(Country::setDescription, "xyz")
						.build(DIALECT, countryTable))
						.getMessage());
	}
	
	@Test
	public void testAdd_mappingAComplextType_throwsException() {
		Table<?> countryTable = new Table<>("countryTable");
		assertEquals("countryTable.timestamp has no matching binder, please consider adding one to dialect binder registry" +
						" or use one of the IFluentEmbeddableMappingConfiguration::embed methods",
				assertThrows(MappingConfigurationException.class, () -> FluentEmbeddableMappingConfigurationSupport.from(Country.class)
						.add(Country::getName)
						.add(Country::setTimestamp)
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
	
	@Test
	public void testEmbed_insertAndSelect_withSomeExcludedProperty() {
		Table<?> personTable = new Table<>("personTable");
		EmbeddedBeanMappingStrategy<Person, Table<?>> personMappingStrategy = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.add(Person::getName)
				.embed(Person::setTimestamp)
					.exclude(Timestamp::getCreationDate)
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
	public void testFluentAPIWriting() {
		Table<?> countryTable = new Table<>("countryTable");
		
		try {
			FluentEmbeddableMappingConfigurationSupport.from(Country.class)
					.add(Country::getName)
					.embed(Country::getPresident)
						.overrideName(Person::getId, "personId")
						.overrideName(Person::getName, "personName")
						.innerEmbed(Person::getTimestamp)
					.embed(Country::getTimestamp)
					.add(Country::getId)
					.add(Country::setDescription, "zxx")
					.mapSuperClass(new EmbeddedBeanMappingStrategy<>(Object.class, new Table<>(""), new HashMap<>()))
					.build(DIALECT, countryTable);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			FluentEmbeddableMappingConfigurationSupport.from(Country.class)
					.add(Country::getName)
					.embed(Country::getPresident)
						.innerEmbed(Person::getTimestamp)
					.embed(Country::getTimestamp)
					.add(Country::getId, "zz")
					.mapSuperClass(new EmbeddedBeanMappingStrategy<>(Object.class, new Table<>(""), new HashMap<>()))
					.add(Country::getDescription, "xx")
					.build(DIALECT, countryTable);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			FluentEmbeddableMappingConfigurationSupport.from(Country.class)
					.add(Country::getName)
					.add(Country::getId, "zz")
					.mapSuperClass(new EmbeddedBeanMappingStrategy<>(Object.class, new Table<>(""), new HashMap<>()))
					// embed with setter
					.embed(Country::setPresident)
						// inner embed with setter
						.innerEmbed(Person::setTimestamp)
					// embed with setter
					.embed(Country::setTimestamp)
					.add(Country::getDescription, "xx")
					.add(Country::getDummyProperty, "dd")
					.build(DIALECT, countryTable);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
					.add(Person::getName);
			
			FluentEmbeddableMappingConfigurationSupport.from(Country.class)
					.add(Country::getName)
					.add(Country::getId, "zz")
					.mapSuperClass(new EmbeddedBeanMappingStrategy<>(Object.class, new Table<>(""), new HashMap<>()))
					// embed with setter
					.embed(Country::getPresident, personMappingBuilder)
					.build(DIALECT, countryTable);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
					.add(Person::getName);
			
			FluentEmbeddableMappingConfigurationSupport.from(Country.class)
					.add(Country::getName)
					.add(Country::getId, "zz")
					.embed(Country::getPresident, personMappingBuilder)
					.mapSuperClass(new EmbeddedBeanMappingStrategy<>(Object.class, new Table<>(""), new HashMap<>()))
					// reusing embeddable ...
					.embed(Country::getPresident, personMappingBuilder)
						// with getter override
						.overrideName(Person::getName, "toto")
						// with setter override
						.overrideName(Person::setName, "tata")
					.build(DIALECT, countryTable);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			FluentEmbeddableMappingConfigurationSupport.from(PersonWithGender.class)
					.add(Person::getName)
					.addEnum(PersonWithGender::getGender).byOrdinal()
					.embed(Person::setTimestamp)
						.overrideName(Timestamp::getCreationDate, "myDate")
					.addEnum(PersonWithGender::getGender, "MM").byOrdinal()
					.add(PersonWithGender::getId, "zz")
					.addEnum(PersonWithGender::setGender).byName()
					.embed(Person::getTimestamp)
					.addEnum(PersonWithGender::setGender, "MM").byName()
					.build(DIALECT, new Table<>("person"));
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
	}
	
	@Test
	public void testBuild_embed_definedTwice_throwException() {
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbedOptions<Country, Timestamp> mappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.embed(Country::getTimestamp)
				.embed(Country::getTimestamp);
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () -> mappingBuilder
				.build(DIALECT, countryTable));
		assertEquals("Country::getTimestamp is already mapped", thrownException.getMessage());
	}
	
	@Test
	public void testBuild_embed() {
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbedOptions<Country, Timestamp> mappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.columnNamingStrategy(accessor -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_col")
				.embed(Country::getTimestamp);
		EmbeddedBeanMappingStrategy<Country, ? extends Table<?>> mappingStrategy = mappingBuilder.build(DIALECT, countryTable);
		
		Map<String, ? extends Column<?, Object>> columnsByName = mappingStrategy.getTargetTable().mapColumnsOnName();
		assertNotNull(columnsByName.get("creationDate_col"));
		assertNotNull(columnsByName.get("modificationDate_col"));
	}
	
	@Test
	public void testBuild_innerEmbed_withTwiceSameInnerEmbeddableName_throwException() {
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbedOptions<Country, Timestamp> mappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident)
					.overrideName(Person::getName, "presidentName")
					.innerEmbed(Person::getTimestamp)
				// this embed will conflict with Country one because its type is already mapped with no override
				.embed(Country::getTimestamp);
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () -> mappingBuilder
				.build(DIALECT, countryTable));
		assertEquals("Country::getTimestamp conflicts with Person::getTimestamp while embedding a o.g.s.p.e.m.Timestamp" +
				", column names should be overriden : j.u.Date o.g.s.p.e.m.Timestamp.getCreationDate(), j.u.Date o.g.s.p.e.m.Timestamp.getModificationDate()",
				thrownException.getMessage());
		
		// we add an override, exception must still be thrown, with different message
		mappingBuilder.overrideName(Timestamp::getModificationDate, "modifiedAt");
		
		thrownException = assertThrows(MappingConfigurationException.class, () -> mappingBuilder
				.build(DIALECT, countryTable));
		assertEquals("Country::getTimestamp conflicts with Person::getTimestamp while embedding a o.g.s.p.e.m.Timestamp" +
				", column names should be overriden : j.u.Date o.g.s.p.e.m.Timestamp.getCreationDate()", thrownException.getMessage());
		
		// we override the last field, no exception is thrown
		mappingBuilder.overrideName(Timestamp::getCreationDate, "createdAt");
		mappingBuilder.build(DIALECT, countryTable);
		
		assertEquals(Arrays.asHashSet(
				// from Country
				"name",
				// from Person
				"id", "version", "presidentName", "creationDate", "modificationDate",
				// from Country.timestamp
				"createdAt", "modifiedAt"),
				countryTable.getColumns().stream().map(Column::getName).collect(Collectors.toSet()));
	}
	
	@Test
	public void testBuild_innerEmbed_withOverridenColumnName() {
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table<?>> personMappingStrategy = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident)
					.overrideName(Person::getName, "presidentName")
					.innerEmbed(Person::getTimestamp)
						.overrideName(Timestamp::getCreationDate, "createdAt")
				.build(DIALECT, countryTable);
		
		Map<String, Column> columnsByName = (Map) personMappingStrategy.getTargetTable().mapColumnsOnName();
		
		assertEquals(Arrays.asHashSet(
				// from Country
				"name",
				// from Person
				"id", "presidentName", "version",
				// from Person.timestamp
				"createdAt", "modificationDate"),
				countryTable.getColumns().stream().map(Column::getName).collect(Collectors.toSet()));
		
		// checking types
		assertEquals(Date.class, columnsByName.get("modificationDate").getJavaType());
		assertEquals(Date.class, columnsByName.get("createdAt").getJavaType());
		assertEquals(String.class, columnsByName.get("presidentName").getJavaType());
	}
	
	@Test
	public void testBuild_withTwiceSameEmbeddableNames_throwsException() {
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbedOptions<Country, Person> mappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident);
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class,
				() -> mappingBuilder.build(DIALECT, countryTable));
		assertEquals("Error while mapping Country::getPresident : o.g.s.p.e.m.Person.name" +
				" conflicts with j.l.String o.g.s.p.e.m.Country.getName() because they use same column," +
				" override one of their name to avoid the conflict, see EmbedOptions::overrideName", thrownException.getMessage());
	}
	
	@Test
	public void testBuild_embedReusedEmbeddable_simpleCase() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.add(Person::getId);
		
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table<?>> personMappingStrategy = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName, "countryName")
				.embed(Country::getPresident, personMappingBuilder)
				.build(DIALECT, countryTable);
		
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
	public void testBuild_embedReusedEmbeddable_simpleCase_setter() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.add(Person::getId);
		
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table<?>> personMappingStrategy = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName, "countryName")
				.embed(Country::setPresident, personMappingBuilder)
				.build(DIALECT, countryTable);
		
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
	public void testBuild_embedReusedEmbeddable_overrideName() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.add(Person::getName)
				.embed(Person::getTimestamp);
		
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table<?>> personMappingStrategy = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident, personMappingBuilder)
					.overrideName(Person::getName, "personName")
				.build(DIALECT, countryTable);
		
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
	public void testBuild_embedReusedEmbeddable_definedTwice_throwException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.add(Person::getName, "myName");
		
		Table<?> countryTable = new Table<>("countryTable");
		
		IFluentEmbeddableMappingBuilder<Country> mappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident, personMappingBuilder)
				// voluntary duplicate to fulfill goal of this test
				.embed(Country::getPresident, personMappingBuilder);
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				mappingBuilder.build(DIALECT, countryTable));
		assertEquals("Country::getPresident is already mapped", thrownException.getMessage());
	}
	
	@Test
	public void testBuild_embedReusedEmbeddable_conflictingColumnNameNotOverriden_throwsException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.add(Person::getName);
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<Country, Person> entityMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident, personMappingBuilder);
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				entityMappingBuilder.build(DIALECT, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator() 
						+ "Embeddable definition 'j.l.String o.g.s.p.e.m.Person.getName()'" 
						+ " vs entity definition 'j.l.String o.g.s.p.e.m.Country.getName()' on column name 'name'",
				thrownException.getMessage());
	}
	
	@Test
	public void testBuild_embedReusedEmbeddable_columnNameOverridenOnConflictingName1_throwsException() {
		// Overriden vs override
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.add(Person::getName, "myName");
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<Country, Person> entityMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName, "myName")
				.embed(Country::getPresident, personMappingBuilder);
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				entityMappingBuilder.build(DIALECT, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator() 
						+ "Embeddable definition 'j.l.String o.g.s.p.e.m.Person.getName()'" 
						+ " vs entity definition 'j.l.String o.g.s.p.e.m.Country.getName()' on column name 'myName'",
				thrownException.getMessage());
	}
	
	@Test
	public void testBuild_embedReusedEmbeddable_columnNameOverridenOnConflictingName2_throwsException() {
		// Overriden vs standard name
		EmbeddedBeanMappingStrategyBuilder<MyPerson> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(MyPerson.class)
				.add(MyPerson::getName, "myName");
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<MyCountry, MyPerson> entityMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(MyCountry.class)
				.add(MyCountry::getMyName)
				.embed(MyCountry::getPresident, personMappingBuilder);
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				entityMappingBuilder.build(DIALECT, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator()
						+ "Embeddable definition 'j.l.String o.g.s.p.e.m.Person.getName()'" 
						+ " vs entity definition 'j.l.String o.g.s.p.e.FluentEmbeddableMappingConfigurationSupportTest$MyCountry.getMyName()' on column name 'myName'",
				thrownException.getMessage());
	}
	
	@Test
	public void testBuild_embedReusedEmbeddable_columnNameOverridenOnConflictingName3_throwsException() {
		// Standard name vs override
		EmbeddedBeanMappingStrategyBuilder<MyPerson> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(MyPerson.class)
				.add(MyPerson::getMyName);
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<MyCountry, MyPerson> entityMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(MyCountry.class)
				.add(Country::getName, "myName")
				.embed(MyCountry::getPresident, personMappingBuilder);
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				entityMappingBuilder.build(DIALECT, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator() 
						+ "Embeddable definition 'j.l.String o.g.s.p.e.FluentEmbeddableMappingConfigurationSupportTest$MyPerson.getMyName()' vs entity definition" 
						+ " 'j.l.String o.g.s.p.e.m.Country.getName()' on column name 'myName'",
				thrownException.getMessage());
	}
	
	@Test
	public void testBuild_embedReusedEmbeddable_overrideNameOfUnmappedProperty_throwsException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.add(Person::getName);
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<Country, Person> entityMappingBuilder =
				FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident, personMappingBuilder)
					.overrideName(Person::getName, "personName")
					.overrideName(Person::getVersion, "personVersion");
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				entityMappingBuilder.build(DIALECT, countryTable));
		assertEquals("Person::getVersion is not mapped by embeddable strategy, so its column name override 'personVersion' can't apply",
				thrownException.getMessage());
	}
	
	@Test
	public void testBuild_embedReusedEmbeddable_embeddableContainsAnEmbeddedType() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.add(Person::getName, "personName")
				.embed(Person::getTimestamp)
					.overrideName(Timestamp::getCreationDate, "personCreatedAt")
					.overrideName(Timestamp::getModificationDate, "personModifiedAt");
		
		
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table<?>> personMappingStrategy = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.embed(Country::getTimestamp)
					.overrideName(Timestamp::getCreationDate, "createdAt")
					.overrideName(Timestamp::getModificationDate, "modifiedAt")
				.embed(Country::getPresident, personMappingBuilder)
					.overrideName(Person::getName, "presidentName")
				.build(DIALECT, countryTable);
		
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
	public void testBuild_embedReusedEmbeddable_embeddableContainsAnEmbeddedType2() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.add(Person::getName, "personName")
				.embed(Person::getTimestamp)
					.overrideName(Timestamp::getCreationDate, "personCreatedAt")
					.exclude(Timestamp::getModificationDate);
		
		
		Table<?> countryTable = new Table<>("countryTable");
		EmbeddedBeanMappingStrategy<Country, Table<?>> personMappingStrategy = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.embed(Country::getTimestamp)
					.overrideName(Timestamp::getCreationDate, "createdAt")
					.overrideName(Timestamp::getModificationDate, "modifiedAt")
				.embed(Country::getPresident, personMappingBuilder)
					.overrideName(Person::getName, "presidentName")
					.overrideName(AccessorChain.chain(Person::getTimestamp, Timestamp::getCreationDate), "presidentElectedAt")
				.build(DIALECT, countryTable);
		
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
	public void testBuild_embedReusedEmbeddable_embeddableContainsAnEmbeddedType_throwsException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.embed(Person::getTimestamp);
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<Country, Person> mappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.embed(Country::getTimestamp)
				.embed(Country::getPresident, personMappingBuilder);
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				mappingBuilder.build(DIALECT, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator()
				+ "Embeddable definition 'Person::getTimestamp > j.u.Date o.g.s.p.e.m.Timestamp.getModificationDate()'"
				+ " vs entity definition 'Country::getTimestamp > j.u.Date o.g.s.p.e.m.Timestamp.getModificationDate()' on column name 'modificationDate'"
				+ System.lineSeparator()
				+ "Embeddable definition 'Person::getTimestamp > j.u.Date o.g.s.p.e.m.Timestamp.getCreationDate()'"
				+ " vs entity definition 'Country::getTimestamp > j.u.Date o.g.s.p.e.m.Timestamp.getCreationDate()' on column name 'creationDate'",
				thrownException.getMessage());
	}
	
	@Test
	public void testBuild_embedReusedEmbeddable_embeddableContainsAnEmbeddedType_withOverridenName_throwsException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.embed(Person::getTimestamp)
				.overrideName(Timestamp::getCreationDate, "createdAt");
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<Country, Person> mappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.embed(Country::getTimestamp)
				.embed(Country::getPresident, personMappingBuilder);
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				mappingBuilder.build(DIALECT, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator()
				+ "Embeddable definition 'Person::getTimestamp > j.u.Date o.g.s.p.e.m.Timestamp.getModificationDate()'"
				+ " vs entity definition 'Country::getTimestamp > j.u.Date o.g.s.p.e.m.Timestamp.getModificationDate()' on column name 'modificationDate'",
				thrownException.getMessage());
	}
	
	@Test
	public void testBuild_embedReusedEmbeddable_embeddableContainsAnEmbeddedType_withExclusion_throwsException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.embed(Person::getTimestamp)
				.exclude(Timestamp::getCreationDate);
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilderEmbeddableOptions<Country, Person> mappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.embed(Country::getTimestamp)
				.embed(Country::getPresident, personMappingBuilder);
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				mappingBuilder.build(DIALECT, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator()
				+ "Embeddable definition 'Person::getTimestamp > j.u.Date o.g.s.p.e.m.Timestamp.getModificationDate()'"
				+ " vs entity definition 'Country::getTimestamp > j.u.Date o.g.s.p.e.m.Timestamp.getModificationDate()' on column name 'modificationDate'",
				thrownException.getMessage());
	}
	
	@Test
	public void testBuild_embedReusedEmbeddable_embeddableContainsAnEmbeddedType_andIsOverriden_throwsException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.embed(Person::getTimestamp);

		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilder<Country> mappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.embed(Country::getTimestamp)
				.embed(Country::getPresident, personMappingBuilder)
					.overrideName(AccessorChain.chain(Person::getTimestamp, Timestamp::getCreationDate), "createdAt");
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				mappingBuilder.build(DIALECT, countryTable));
		assertEquals("Some embedded columns conflict with entity ones on their name, please override it or change it :" + System.lineSeparator()
				+ "Embeddable definition 'Person::getTimestamp > j.u.Date o.g.s.p.e.m.Timestamp.getModificationDate()'"
				+ " vs entity definition 'Country::getTimestamp > j.u.Date o.g.s.p.e.m.Timestamp.getModificationDate()' on column name 'modificationDate'",
				thrownException.getMessage());
	}
	
	@Test
	public void testBuild_embedReusedEmbeddable__embeddableContainsAnEmbeddedType_overrideNameOfUnmappedProperty_throwsException() {
		EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
				.embed(Person::getTimestamp)
					.exclude(Timestamp::getModificationDate);
		
		Table<?> countryTable = new Table<>("countryTable");
		IFluentEmbeddableMappingBuilder<Country> mappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Country.class)
				.add(Country::getName)
				.embed(Country::getPresident, personMappingBuilder)
					.overrideName(AccessorChain.chain(Person::getTimestamp, Timestamp::getModificationDate), "electedAt");
		
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () ->
				mappingBuilder.build(DIALECT, countryTable));
		assertEquals("Person::getTimestamp > Timestamp::getModificationDate is not mapped by embeddable strategy, so its column name override 'electedAt' can't apply",
				thrownException.getMessage());
	}
	
	@Test
	public void testBuild_withEnumType() {
		Table<?> personTable = new Table<>("personTable");
		EmbeddedBeanMappingStrategy<PersonWithGender, Table> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(PersonWithGender.class)
				.add(Person::getName)
				.addEnum(PersonWithGender::getGender)
				.build(DIALECT, personTable);
		
		PersonWithGender person = new PersonWithGender();
		person.setName("toto");
		person.setGender(Gender.FEMALE);
		Map<Column<Table, Object>, Object> insertValues = personMappingBuilder.getInsertValues(person);
		assertEquals("toto", insertValues.get(personTable.mapColumnsOnName().get("name")));
		assertEquals(Gender.FEMALE, insertValues.get(personTable.mapColumnsOnName().get("gender")));
		
		ParameterBinder genderColumnBinder = DIALECT.getColumnBinderRegistry().getBinder(personTable.mapColumnsOnName().get("gender"));
		// by default, gender will be mapped on its name
		assertTrue(genderColumnBinder instanceof NameEnumParameterBinder);
		assertEquals(Gender.class, ((NameEnumParameterBinder) genderColumnBinder).getEnumType());
		
		
		// changing mapping to ordinal
		personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(PersonWithGender.class)
				.add(Person::getName)
				.addEnum(PersonWithGender::getGender).byOrdinal()
				.build(DIALECT, personTable);
		
		genderColumnBinder = DIALECT.getColumnBinderRegistry().getBinder(personTable.mapColumnsOnName().get("gender"));
		// by default, gender will be mapped on its name
		assertTrue(genderColumnBinder instanceof OrdinalEnumParameterBinder);
		assertEquals(Gender.class, ((OrdinalEnumParameterBinder) genderColumnBinder).getEnumType());
		
		// changing mapping to name
		personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(PersonWithGender.class)
				.add(Person::getName)
				.addEnum(PersonWithGender::getGender).byName()
				.build(DIALECT, personTable);
		
		genderColumnBinder = DIALECT.getColumnBinderRegistry().getBinder(personTable.mapColumnsOnName().get("gender"));
		// by default, gender will be mapped on its name
		assertTrue(genderColumnBinder instanceof NameEnumParameterBinder);
		assertEquals(Gender.class, ((NameEnumParameterBinder) genderColumnBinder).getEnumType());
	}
	
	static public class MyPerson extends Person {
		private String myName;
		
		public String getMyName() {
			return myName;
		}
	}
	
	static public class MyCountry extends Country {
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