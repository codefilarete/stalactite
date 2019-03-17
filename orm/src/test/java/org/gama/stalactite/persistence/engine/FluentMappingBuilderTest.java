package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.gama.lang.InvocationHandlerSupport;
import org.gama.lang.Reflections;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.sql.SimpleConnectionProvider;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.dml.SQLOperation.SQLOperationListener;
import org.gama.sql.dml.SQLStatement;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderColumnOptions;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderEmbedOptions;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.engine.model.Timestamp;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilderTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	
	@BeforeAll
	public static void initBinders() {
		DIALECT.getDmlGenerator().sortColumnsAlphabetically();	// for steady checks on SQL orders
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
	}
	
	@Test
	public void testBuild_identifierIsNotDefined_throwsException() {
		IFluentMappingBuilderColumnOptions<Toto, StatefullIdentifier> mappingStrategy = FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId)
				.add(Toto::getName);
		
		// column should be correctly created
		assertEquals("Identifier is not defined, please add one throught " +
						"o.g.s.p.e.IFluentMappingBuilder o.g.s.p.e.ColumnOptions.identifier(o.g.s.p.e.FluentMappingBuilder$IdentifierPolicy)",
				assertThrows(UnsupportedOperationException.class, () -> mappingStrategy.build(DIALECT))
						.getMessage());
	}
	
	@Test
	public void testAdd_withoutName_targetedPropertyNameIsTaken() {
		ClassMappingStrategy<Toto, StatefullIdentifier, Table> mappingStrategy = FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Toto::getName)
				.build(DIALECT);
		
		// column should be correctly created
		assertEquals("Toto", mappingStrategy.getTargetTable().getName());
		Column columnForProperty = (Column) mappingStrategy.getTargetTable().mapColumnsOnName().get("name");
		assertNotNull(columnForProperty);
		assertEquals(String.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testAdd_withColumn_columnIsTaken() {
		Table toto = new Table("Toto");
		Column<Table, String> titleColumn = toto.addColumn("title", String.class);
		ClassMappingStrategy<Toto, StatefullIdentifier, Table> mappingStrategy = FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Toto::getName, titleColumn)
				.build(DIALECT);
		
		// column should not have been created
		Column columnForProperty = (Column) mappingStrategy.getTargetTable().mapColumnsOnName().get("name");
		assertNull(columnForProperty);
	}
	
	@Test
	public void testAdd_definedAsIdentifier_columnBecomesPrimaryKey() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
			.add(Toto::getName).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
			.build(DIALECT);
		// column should be correctly created
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("name");
		assertTrue(columnForProperty.isPrimaryKey());
	}
	
	@Test
	public void testAdd_identifierDefinedTwice_throwsException() throws NoSuchMethodException {
		Table toto = new Table("Toto");
		assertEquals("Identifier is already defined by " + Reflections.toString(Toto.class.getMethod("getName")),
				assertThrows(IllegalArgumentException.class, () -> FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
						.add(Toto::getName, "tata").identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.add(Toto::getFirstName).identifier(IdentifierPolicy.ALREADY_ASSIGNED))
						.getMessage());
	}
	
	@Test
	public void testAdd_mappingDefinedTwiceByMethod_throwsException() throws NoSuchMethodException {
		Table toto = new Table("Toto");
		assertEquals("Mapping is already defined by method " + Reflections.toString(Toto.class.getMethod("getName")),
				assertThrows(MappingConfigurationException.class, () -> FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
						.add(Toto::getName)
						.add(Toto::setName))
						.getMessage());
	}
	
	@Test
	public void testAdd_mappingDefinedTwiceByColumn_throwsException() {
		Table toto = new Table("Toto");
		assertEquals("Mapping is already defined for column xyz",
				assertThrows(MappingConfigurationException.class, () -> FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
						.add(Toto::getName, "xyz")
						.add(Toto::getFirstName, "xyz"))
						.getMessage());
	}
	
	@Test
	public void testAdd_methodHasNoMatchingField_configurationIsStillValid() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Toto::getNoMatchingField)
				.build(DIALECT);
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("noMatchingField");
		assertNotNull(columnForProperty);
		assertEquals(Long.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testAdd_methodIsASetter_configurationIsStillValid() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::setId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.build(DIALECT);
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("id");
		assertNotNull(columnForProperty);
		assertEquals(Identifier.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testEmbed_definedByGetter() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp)
				.build(new PersistenceContext(null, DIALECT));
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("creationDate");
		assertNotNull(columnForProperty);
		assertEquals(Date.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testEmbed_definedBySetter() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Toto::setTimestamp)
				.build(new PersistenceContext(null, DIALECT));
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("creationDate");
		assertNotNull(columnForProperty);
		assertEquals(Date.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testEmbed_withOverridenColumnName() {
		Table toto = new Table("Toto");
		FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, toto)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp)
					.overrideName(Timestamp::getCreationDate, "createdAt")
					.overrideName(Timestamp::getModificationDate, "modifiedAt")
				.build(new PersistenceContext(null, DIALECT));
		
		Map<String, Column> columnsByName = toto.mapColumnsOnName();
		
		// columns with getter name must be absent (hard to test: can be absent for many reasons !)
		assertNull(columnsByName.get("creationDate"));
		assertNull(columnsByName.get("modificationDate"));

		Column overridenColumn;
		// Columns with good name must be present
		overridenColumn = columnsByName.get("modifiedAt");
		assertNotNull(overridenColumn);
		assertEquals(Date.class, overridenColumn.getJavaType());
		overridenColumn = columnsByName.get("createdAt");
		assertNotNull(overridenColumn);
		assertEquals(Date.class, overridenColumn.getJavaType());
	}
	
	@Test
	public void testEmbed_withOverridenColumn() {
		Table targetTable = new Table("Toto");
		Column<Table, Date> createdAt = targetTable.addColumn("createdAt", Date.class);
		Column<Table, Date> modifiedAt = targetTable.addColumn("modifiedAt", Date.class);
		
		Connection connectionMock = InvocationHandlerSupport.mock(Connection.class);
		
		Persister<Toto, StatefullIdentifier, ?> persister = FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, targetTable)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp)
					.override(Timestamp::getCreationDate, createdAt)
					.override(Timestamp::getModificationDate, modifiedAt)
				.build(new PersistenceContext(new SimpleConnectionProvider(connectionMock), DIALECT));
		
		Map<String, Column> columnsByName = targetTable.mapColumnsOnName();
		
		// columns with getter name must be absent (hard to test: can be absent for many reasons !)
		assertNull(columnsByName.get("creationDate"));
		assertNull(columnsByName.get("modificationDate"));
		
		// checking that overriden column are in DML statements
		assertEquals(targetTable.getColumns(), persister.getMappingStrategy().getInsertableColumns());
		assertEquals(targetTable.getColumnsNoPrimaryKey(), persister.getMappingStrategy().getUpdatableColumns());
		assertEquals(targetTable.getColumns(), persister.getMappingStrategy().getSelectableColumns());
	}
	
	@Test
	public void testEmbed_withSomeExcludedProperty() {
		Table targetTable = new Table("Toto");
		Column<Table, Date> modifiedAt = targetTable.addColumn("modifiedAt", Date.class);
		
		Connection connectionMock = InvocationHandlerSupport.mock(Connection.class);
		
		Persister<Toto, StatefullIdentifier, ?> persister = FluentMappingBuilder.from(Toto.class, StatefullIdentifier.class, targetTable)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp)
					.exclude(Timestamp::getCreationDate)
					.override(Timestamp::getModificationDate, modifiedAt)
				.build(new PersistenceContext(new SimpleConnectionProvider(connectionMock), DIALECT));
		
		Map<String, Column> columnsByName = targetTable.mapColumnsOnName();
		
		// columns with getter name must be absent (hard to test: can be absent for many reasons !)
		assertNull(columnsByName.get("creationDate"));
		assertNull(columnsByName.get("modificationDate"));
		
		// checking that overriden column are in DML statements
		assertEquals(targetTable.getColumns(), persister.getMappingStrategy().getInsertableColumns());
		assertEquals(targetTable.getColumnsNoPrimaryKey(), persister.getMappingStrategy().getUpdatableColumns());
		assertEquals(targetTable.getColumns(), persister.getMappingStrategy().getSelectableColumns());
	}
	
	@Test
	public void testBuild_innerEmbed_withSomeExcludedProperty() throws SQLException {
		Table<?> countryTable = new Table<>("countryTable");
		
		IFluentMappingBuilderEmbedOptions<Country, StatefullIdentifier, Timestamp> mappingBuilder = FluentMappingBuilder.from(Country.class,
				StatefullIdentifier.class, countryTable)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.embed(Country::getPresident)
					.exclude(Person::getId)
					.exclude(Person::getVersion)
					.overrideName(Person::getName, "presidentName")
					.innerEmbed(Person::getTimestamp)
						.exclude(Timestamp::getCreationDate)
						.overrideName(Timestamp::getModificationDate, "persidentElectedAt")
				// this embed will conflict with Country one because its type is already mapped with no override
				.embed(Country::getTimestamp)
					.exclude(Timestamp::getModificationDate)
					.overrideName(Timestamp::getCreationDate, "countryCreatedAt");
		
		mappingBuilder.build(DIALECT);
		
		assertEquals(Arrays.asHashSet(
				// from Country
				"id", "name",
				// from Person
				"presidentName", "persidentElectedAt",
				// from Country.timestamp
				"countryCreatedAt"),
				countryTable.getColumns().stream().map(Column::getName).collect(Collectors.toSet()));
		
		Connection connectionMock = mock(Connection.class);
		
		
		Persister<Country, StatefullIdentifier, Table> persister = mappingBuilder.build(
				new PersistenceContext(new SimpleConnectionProvider(connectionMock), DIALECT));
		
		Map<String, ? extends Column> columnsByName = countryTable.mapColumnsOnName();
		
		// columns with getter name must be absent (hard to test: can be absent for many reasons !)
		assertNull(columnsByName.get("creationDate"));
		assertNull(columnsByName.get("modificationDate"));
		
		// checking that overriden column are in DML statements
		assertEquals(countryTable.getColumns(), persister.getMappingStrategy().getInsertableColumns());
		assertEquals(countryTable.getColumnsNoPrimaryKey(), persister.getMappingStrategy().getUpdatableColumns());
		assertEquals(countryTable.getColumns(), persister.getMappingStrategy().getSelectableColumns());
		
		Country country = new Country(new PersistableIdentifier<>(1L));
		country.setName("France");
		
		Timestamp countryTimestamp = new Timestamp();
		LocalDateTime localDateTime = LocalDate.of(2018, 01, 01).atStartOfDay();
		Date countryCreationDate = Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
		countryTimestamp.setCreationDate(countryCreationDate);
		country.setTimestamp(countryTimestamp);
		
		Person president = new Person();
		president.setName("François");
		
		Timestamp presidentTimestamp = new Timestamp();
		Date presidentElection = Date.from(LocalDate.of(2019, 01, 01).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
		presidentTimestamp.setModificationDate(presidentElection);
		president.setTimestamp(presidentTimestamp);
		
		country.setPresident(president);
		
		// preparing JDBC mocks and values capture
		PreparedStatement statementMock = mock(PreparedStatement.class);
		when(statementMock.executeBatch()).thenReturn(new int[] { 1 });
		Map<Column<Table, Object>, Object> capturedValues = new HashMap<>();
		when(connectionMock.prepareStatement(anyString())).thenReturn(statementMock);
		
		StringBuilder capturedSQL = new StringBuilder();
		persister.getInsertExecutor().setOperationListener(new SQLOperationListener<Column<Table, Object>>() {
			@Override
			public void onValuesSet(Map<Column<Table, Object>, ?> values) {
				capturedValues.putAll(values);
			}
			
			@Override
			public void onExecute(SQLStatement<Column<Table, Object>> sqlStatement) {
				capturedSQL.append(sqlStatement.getSQL());
			}
		});
		
		// Testing ...
		persister.insert(country);
		
		assertEquals("insert into countryTable(countryCreatedAt, id, name, persidentElectedAt, presidentName) values (?, ?, ?, ?, ?)",
				capturedSQL.toString());
		assertEquals(Maps.asHashMap((Column) columnsByName.get("presidentName"), (Object) country.getPresident().getName())
				.add(columnsByName.get("persidentElectedAt"), country.getPresident().getTimestamp().getModificationDate())
				.add(columnsByName.get("name"), country.getName())
				.add(columnsByName.get("countryCreatedAt"), country.getTimestamp().getCreationDate())
				.add(columnsByName.get("id"), country.getId())
				, capturedValues);
	}
	
	@Test
	public void testBuild_innerEmbed_withTwiceSameInnerEmbeddableName() {
		Table<?> countryTable = new Table<>("countryTable");
		IFluentMappingBuilderEmbedOptions<Country, StatefullIdentifier, Timestamp> mappingBuilder = FluentMappingBuilder.from(Country.class,
				StatefullIdentifier.class, countryTable)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.embed(Country::getPresident)
					.overrideName(Person::getId, "presidentId")
					.overrideName(Person::getName, "presidentName")
					.innerEmbed(Person::getTimestamp)
				// this embed will conflict with Country one because its type is already mapped with no override
				.embed(Country::getTimestamp);
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () -> mappingBuilder
				.build(DIALECT));
		assertEquals("Country::getTimestamp conflicts with Person::getTimestamp while embedding a o.g.s.p.e.m.Timestamp" +
				", column names should be overriden : j.u.Date o.g.s.p.e.m.Timestamp.getCreationDate(), j.u.Date o.g.s.p.e.m.Timestamp.getModificationDate()", thrownException.getMessage());
		
		// we add an override, exception must still be thrown, with different message
		mappingBuilder.overrideName(Timestamp::getModificationDate, "modifiedAt");
		
		thrownException = assertThrows(MappingConfigurationException.class, () -> mappingBuilder
				.build(DIALECT));
		assertEquals("Country::getTimestamp conflicts with Person::getTimestamp while embedding a o.g.s.p.e.m.Timestamp" +
				", column names should be overriden : j.u.Date o.g.s.p.e.m.Timestamp.getCreationDate()", thrownException.getMessage());
		
		// we override the last field, no exception is thrown
		mappingBuilder.overrideName(Timestamp::getCreationDate, "createdAt");
		mappingBuilder.build(DIALECT);
		
		assertEquals(Arrays.asHashSet(
				// from Country
				"id", "name",
				// from Person
				"presidentId", "version", "presidentName", "creationDate", "modificationDate",
				// from Country.timestamp
				"createdAt", "modifiedAt"),
				countryTable.getColumns().stream().map(Column::getName).collect(Collectors.toSet()));
	}
	
	protected static class Toto implements Identified<Integer> {
		
		private String name;
		
		private String firstName;
		
		private Timestamp timestamp;
		
		public Toto() {
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public String getFirstName() {
			return name;
		}
		
		public Long getNoMatchingField() {
			return null;
		}
		
		public void setNoMatchingField(Long s) {
		}
		
		public long getNoMatchingFieldPrimitive() {
			return 0;
		}
		
		public void setNoMatchingFieldPrimitive(long s) {
		}
		
		@Override
		public Identifier<Integer> getId() {
			return null;
		}
		
		public void setId(Identifier<Integer> id) {
			
		}
		
		public Timestamp getTimestamp() {
			return timestamp;
		}
		
		public void setTimestamp(Timestamp timestamp) {
			this.timestamp = timestamp;
		}
	}
	
}
