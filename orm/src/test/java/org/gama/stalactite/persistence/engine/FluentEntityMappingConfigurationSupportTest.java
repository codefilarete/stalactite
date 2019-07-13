package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.gama.lang.Duo;
import org.gama.lang.InvocationHandlerSupport;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.sql.SimpleConnectionProvider;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.dml.SQLOperation.SQLOperationListener;
import org.gama.sql.dml.SQLStatement;
import org.gama.sql.dml.SQLStatement.BindingException;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.IFluentEntityMappingBuilder.IFluentMappingBuilderEmbedOptions;
import org.gama.stalactite.persistence.engine.IFluentEntityMappingBuilder.IFluentMappingBuilderPropertyOptions;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Gender;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.engine.model.PersonWithGender;
import org.gama.stalactite.persistence.engine.model.Timestamp;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.gama.lang.collection.Iterables.collect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
public class FluentEntityMappingConfigurationSupportTest {
	
	// NB: dialect is made non static because we register binder for the same column several times in these tests
	// and this is not supported : the very first one takes priority  
	private HSQLDBDialect dialect = new HSQLDBDialect();
	private DataSource dataSource = new HSQLDBInMemoryDataSource();
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void initTest() {
		dialect.getDmlGenerator().sortColumnsAlphabetically();	// for steady checks on SQL orders
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		dialect.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
		
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), dialect);
	}
	
	@Test
	public void build_identifierIsNotDefined_throwsException() {
		IFluentMappingBuilderPropertyOptions<Toto, StatefullIdentifier> mappingStrategy = MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId)
				.add(Toto::getName);
		
		// column should be correctly created
		assertEquals("Identifier is not defined for o.g.s.p.e.FluentEntityMappingConfigurationSupportTest$Toto," 
						+ " please add one throught o.g.s.p.e.ColumnOptions.identifier(o.g.s.p.e.ColumnOptions$IdentifierPolicy)",
				assertThrows(UnsupportedOperationException.class, () -> mappingStrategy.build(persistenceContext))
						.getMessage());
	}
	
	@Test
	public void add_withoutName_targetedPropertyNameIsTaken() {
		Persister<Toto, StatefullIdentifier, Table> persister = MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Toto::getName)
				.build(persistenceContext);
		
		// column should be correctly created
		assertEquals("Toto", persister.getMainTable().getName());
		Column columnForProperty = (Column) persister.getMainTable().mapColumnsOnName().get("name");
		assertNotNull(columnForProperty);
		assertEquals(String.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void add_mandatory_onMissingValue_throwsException() {
		Table totoTable = new Table("Toto");
		Column idColumn = totoTable.addColumn("id", StatefullIdentifier.class);
		dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_PARAMETER_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(idColumn, "VARCHAR(255)");
		
		Persister<Toto, StatefullIdentifier, Table> persister = MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Toto::getName).mandatory()
				.add(Toto::getFirstName).mandatory()
				.build(persistenceContext);
		
		// column should be correctly created
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Toto toto = new Toto();
		RuntimeException thrownException = assertThrows(RuntimeException.class, () -> persister.insert(toto));
		assertEquals("Error while inserting values for " + toto, thrownException.getMessage());
		assertEquals(BindingException.class, thrownException.getCause().getClass());
		assertEquals("Expected non null value for : Toto.firstName, Toto.name", thrownException.getCause().getMessage());
	}
	
	@Test
	public void add_mandatory_columnConstraintIsAdded() throws SQLException {
		Persister<Toto, StatefullIdentifier, Table> totoPersister = MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Toto::getName).mandatory()
				.build(persistenceContext);
		
		assertFalse(totoPersister.getMainTable().getColumn("name").isNullable());
	}
	
	@Test
	public void add_withColumn_columnIsTaken() {
		Table toto = new Table("Toto");
		Column<Table, String> titleColumn = toto.addColumn("title", String.class);
		Persister<Toto, StatefullIdentifier, Table> persister = MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Toto::getName, titleColumn)
				.build(persistenceContext);
		
		// column should not have been created
		Column columnForProperty = (Column) persister.getMainTable().mapColumnsOnName().get("name");
		assertNull(columnForProperty);
		
		// title column is expected to be added to the mapping and participate to DML actions 
		assertEquals(Arrays.asSet("id", "title"), persister.getMappingStrategy().getInsertableColumns().stream().map(Column::getName).collect(Collectors.toSet()));
		assertEquals(Arrays.asSet("title"), persister.getMappingStrategy().getUpdatableColumns().stream().map(Column::getName).collect(Collectors.toSet()));
	}
	
	@Test
	public void add_definedAsIdentifier_alreadyAssignedButDoesntImplementIdentifier_throwsException() {
		NotYetSupportedOperationException thrownException = assertThrows(NotYetSupportedOperationException.class,
				() -> MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
				.add(Toto::getName).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.build(persistenceContext));
		assertEquals("Already-assigned identifier policy is only supported with entities that implement o.g.s.p.i.Identified", thrownException.getMessage());
	}
	
	@Test
	public void add_definedAsIdentifier_identifierIsStoredAsString() {
		HSQLDBDialect dialect = new HSQLDBDialect();
		PersistenceContext persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), dialect);
		
		Table totoTable = new Table("Toto");
		Column id = totoTable.addColumn("id", Identifier.class).primaryKey();
		Column name = totoTable.addColumn("name", String.class);
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register(id, Identifier.identifierBinder(DefaultParameterBinders.UUID_PARAMETER_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(id, "varchar(255)");
		
		Persister<Toto, StatefullIdentifier, Table> persister = MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.build(persistenceContext, totoTable);
		// column should be correctly created
		assertTrue(totoTable.getColumn("id").isPrimaryKey());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Toto toto = new Toto();
		toto.setName("toto");
		persister.persist(toto);
		
		List<Duo> select = persistenceContext.select(Duo::new, id, name);
		assertEquals(1, select.size());
		assertEquals(toto.getId().getSurrogate().toString(), ((Identifier) select.get(0).getLeft()).getSurrogate().toString());
	}
	
	@Test
	public void add_identifierDefinedTwice_throwsException() {
		assertEquals("Identifier is already defined by Toto::getId",
				assertThrows(IllegalArgumentException.class, () -> MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
						.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.add(Toto::getIdentifier).identifier(IdentifierPolicy.ALREADY_ASSIGNED))
						.getMessage());
	}
	
	@Test
	public void add_mappingDefinedTwiceByMethod_throwsException() {
		assertEquals("Mapping is already defined by method Toto::getName",
				assertThrows(MappingConfigurationException.class, () -> MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
						.add(Toto::getName)
						.add(Toto::setName))
						.getMessage());
	}
	
	@Test
	public void add_mappingDefinedTwiceByColumn_throwsException() {
		assertEquals("Mapping is already defined for column xyz",
				assertThrows(MappingConfigurationException.class, () -> MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
						.add(Toto::getName, "xyz")
						.add(Toto::getFirstName, "xyz"))
						.getMessage());
	}
	
	@Test
	public void add_methodHasNoMatchingField_configurationIsStillValid() {
		Table toto = new Table("Toto");
		MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Toto::getNoMatchingField)
				.build(persistenceContext, toto);
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("noMatchingField");
		assertNotNull(columnForProperty);
		assertEquals(Long.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void add_methodIsASetter_configurationIsStillValid() {
		Table toto = new Table("Toto");
		MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
				.add(Toto::setId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.build(persistenceContext, toto);
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("id");
		assertNotNull(columnForProperty);
		assertEquals(Identifier.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testEmbed_definedByGetter() {
		Table toto = new Table("Toto");
		MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp)
				.build(new PersistenceContext(null, dialect), toto);
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("creationDate");
		assertNotNull(columnForProperty);
		assertEquals(Date.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testEmbed_definedBySetter() {
		Table toto = new Table("Toto");
		MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Toto::setTimestamp)
				.build(new PersistenceContext(null, dialect), toto);
		
		Column columnForProperty = (Column) toto.mapColumnsOnName().get("creationDate");
		assertNotNull(columnForProperty);
		assertEquals(Date.class, columnForProperty.getJavaType());
	}
	
	@Test
	public void testEmbed_withOverridenColumnName() {
		Table toto = new Table("Toto");
		MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp)
					.overrideName(Timestamp::getCreationDate, "createdAt")
					.overrideName(Timestamp::getModificationDate, "modifiedAt")
				.build(new PersistenceContext(null, dialect), toto);
		
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
		
		Persister<Toto, StatefullIdentifier, ?> persister = MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp)
					.override(Timestamp::getCreationDate, createdAt)
					.override(Timestamp::getModificationDate, modifiedAt)
				.build(new PersistenceContext(new SimpleConnectionProvider(connectionMock), dialect), targetTable);
		
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
		
		Persister<Toto, StatefullIdentifier, ?> persister = MappingEase.mappingBuilder(Toto.class, StatefullIdentifier.class)
				.add(Toto::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.embed(Toto::getTimestamp)
					.exclude(Timestamp::getCreationDate)
					.override(Timestamp::getModificationDate, modifiedAt)
				.build(new PersistenceContext(new SimpleConnectionProvider(connectionMock), dialect), targetTable);
		
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
	public void build_innerEmbed_withSomeExcludedProperty() throws SQLException {
		Table<?> countryTable = new Table<>("countryTable");
		
		IFluentMappingBuilderEmbedOptions<Country, StatefullIdentifier, Timestamp> mappingBuilder = MappingEase
				.mappingBuilder(Country.class, StatefullIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.embed(Country::getPresident)
					.exclude(Person::getId)
					.exclude(Person::getVersion)
					.exclude(Person::getCountry)
					.overrideName(Person::getName, "presidentName")
					.innerEmbed(Person::getTimestamp)
						.exclude(Timestamp::getCreationDate)
						.overrideName(Timestamp::getModificationDate, "presidentElectedAt")
				// this embed will conflict with Country one because its type is already mapped with no override
				.embed(Country::getTimestamp)
					.exclude(Timestamp::getModificationDate)
					.overrideName(Timestamp::getCreationDate, "countryCreatedAt");
		
		mappingBuilder.build(persistenceContext, countryTable);
		
		assertEquals(Arrays.asHashSet(
				// from Country
				"id", "name",
				// from Person
				"presidentName", "presidentElectedAt",
				// from Country.timestamp
				"countryCreatedAt"),
				collect(countryTable.getColumns(), Column::getName, HashSet::new));
		
		Connection connectionMock = mock(Connection.class);
		
		
		Persister<Country, StatefullIdentifier, Table> persister = mappingBuilder.build(
				new PersistenceContext(new SimpleConnectionProvider(connectionMock), dialect), countryTable);
		
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
		
		assertEquals("insert into countryTable(countryCreatedAt, id, name, presidentElectedAt, presidentName) values (?, ?, ?, ?, ?)",
				capturedSQL.toString());
		assertEquals(Maps.asHashMap((Column) columnsByName.get("presidentName"), (Object) country.getPresident().getName())
				.add(columnsByName.get("presidentElectedAt"), country.getPresident().getTimestamp().getModificationDate())
				.add(columnsByName.get("name"), country.getName())
				.add(columnsByName.get("countryCreatedAt"), country.getTimestamp().getCreationDate())
				.add(columnsByName.get("id"), country.getId())
				, capturedValues);
	}
	
	@Test
	public void build_innerEmbed_withTwiceSameInnerEmbeddableName() {
		Table<?> countryTable = new Table<>("countryTable");
		IFluentMappingBuilderEmbedOptions<Country, StatefullIdentifier, Timestamp> mappingBuilder = MappingEase.mappingBuilder(Country.class,
				StatefullIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.embed(Country::getPresident)
					.exclude(Person::getCountry)
					.overrideName(Person::getId, "presidentId")
					.overrideName(Person::getName, "presidentName")
					.innerEmbed(Person::getTimestamp)
				// this embed will conflict with Country one because its type is already mapped with no override
				.embed(Country::getTimestamp);
		MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class, () -> mappingBuilder
				.build(persistenceContext, countryTable));
		assertEquals("Country::getTimestamp conflicts with Person::getTimestamp while embedding a o.g.s.p.e.m.Timestamp" +
				", column names should be overriden : o.g.s.p.e.m.Timestamp.getCreationDate(), o.g.s.p.e.m.Timestamp.getModificationDate()", thrownException.getMessage());
		
		// we add an override, exception must still be thrown, with different message
		mappingBuilder.overrideName(Timestamp::getModificationDate, "modifiedAt");
		
		thrownException = assertThrows(MappingConfigurationException.class, () -> mappingBuilder
				.build(persistenceContext));
		assertEquals("Country::getTimestamp conflicts with Person::getTimestamp while embedding a o.g.s.p.e.m.Timestamp" +
				", column names should be overriden : o.g.s.p.e.m.Timestamp.getCreationDate()", thrownException.getMessage());
		
		// we override the last field, no exception is thrown
		mappingBuilder.overrideName(Timestamp::getCreationDate, "createdAt");
		mappingBuilder.build(persistenceContext, countryTable);
		
		assertEquals(Arrays.asHashSet(
				// from Country
				"id", "name",
				// from Person
				"presidentId", "version", "presidentName", "creationDate", "modificationDate",
				// from Country.timestamp
				"createdAt", "modifiedAt"),
				collect(countryTable.getColumns(), Column::getName, HashSet::new));
	}
	
	
	@Test
	public void build_withEnum_byDefault_nameIsUsed() {
		Persister<PersonWithGender, Identifier<Long>, Table> personPersister = MappingEase.mappingBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName)
				// no type of mapping given: neither ordinal nor name
				.addEnum(PersonWithGender::getGender)
				.build(persistenceContext);
		
		Column gender = (Column) personPersister.getMainTable().mapColumnsOnName().get("gender");
		dialect.getJavaTypeToSqlTypeMapping().put(gender, "VARCHAR(255)");
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersonWithGender person = new PersonWithGender(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.setGender(Gender.FEMALE);
		personPersister.insert(person);
		
		// checking that name was used
		List<String> result = persistenceContext.newQuery("select * from PersonWithGender", String.class).mapKey(String::new, "gender", String.class)
				.execute();
		assertEquals(Arrays.asList("FEMALE"), result);
	}
	
	@Test
	public void addEnum_mandatory_onMissingValue_throwsException() {
		Persister<PersonWithGender, Identifier<Long>, Table> personPersister = MappingEase.mappingBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName)
				// no type of mapping given: neither ordinal nor name
				.addEnum(PersonWithGender::getGender).mandatory()
				.build(persistenceContext);
		
		Column gender = (Column) personPersister.getMainTable().mapColumnsOnName().get("gender");
		dialect.getJavaTypeToSqlTypeMapping().put(gender, "VARCHAR(255)");
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersonWithGender person = new PersonWithGender(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.setGender(null);
		
		RuntimeException thrownException = assertThrows(RuntimeException.class, () -> personPersister.insert(person));
		assertEquals("Error while inserting values for " + person, thrownException.getMessage());
		assertEquals(BindingException.class, thrownException.getCause().getClass());
		assertEquals("Expected non null value for : PersonWithGender.gender", thrownException.getCause().getMessage());
	}
	
	@Test
	public void addEnum_mandatory_columnConstraintIsAdded() throws SQLException {
		Persister<PersonWithGender, Identifier<Long>, Table> personPersister = MappingEase.mappingBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName)
				// no type of mapping given: neither ordinal nor name
				.addEnum(PersonWithGender::getGender).mandatory()
				.build(persistenceContext);
		
		assertFalse(personPersister.getMainTable().getColumn("gender").isNullable());
	}
	
	@Test
	public void build_withEnum_mappedWithOrdinal() {
		Persister<PersonWithGender, Identifier<Long>, Table> personPersister = MappingEase.mappingBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName)
				.addEnum(PersonWithGender::getGender).byOrdinal()
				.build(persistenceContext);
		
		Column gender = (Column) personPersister.getMainTable().mapColumnsOnName().get("gender");
		dialect.getJavaTypeToSqlTypeMapping().put(gender, "INT");
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersonWithGender person = new PersonWithGender(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.setGender(Gender.FEMALE);
		personPersister.insert(person);
		
		// checking that ordinal was used
		List<Integer> result = persistenceContext.newQuery("select * from PersonWithGender", Integer.class).mapKey(Integer::valueOf, "gender", String.class)
				.execute();
		assertEquals(Arrays.asList(person.getGender().ordinal()), result);
	}
	
	@Test
	public void build_withEnum_columnMappedWithOrdinal() {
		Table personTable = new Table<>("PersonWithGender");
		Column<Table, Gender> genderColumn = personTable.addColumn("gender", Gender.class);
		
		Persister<PersonWithGender, Identifier<Long>, ?> personPersister = MappingEase.mappingBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName)
				.addEnum(PersonWithGender::getGender, genderColumn).byOrdinal()
				.build(persistenceContext);
		
		dialect.getJavaTypeToSqlTypeMapping().put(genderColumn, "INT");
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersonWithGender person = new PersonWithGender(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.setGender(Gender.FEMALE);
		personPersister.insert(person);
		
		// checking that ordinal was used
		List<Integer> result = persistenceContext.newQuery("select * from PersonWithGender", Integer.class).mapKey(Integer::valueOf, "gender", String.class)
				.execute();
		assertEquals(Arrays.asList(person.getGender().ordinal()), result);
	}
	
	@Test
	public void insert() {
		Persister<PersonWithGender, Identifier<Long>, Table> personPersister = MappingEase.mappingBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName)
				// no type of mapping given: neither ordinal nor name
				.addEnum(PersonWithGender::getGender)
				.build(persistenceContext);
		
		Column gender = (Column) personPersister.getMainTable().mapColumnsOnName().get("gender");
		dialect.getJavaTypeToSqlTypeMapping().put(gender, "VARCHAR(255)");
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersonWithGender person = new PersonWithGender(new PersistableIdentifier<>(1L));
		person.setName(null);
		person.setGender(null);
		
		personPersister.insert(person);
	}
	
	@Test
	public void insert_nullValues() {
		Persister<PersonWithGender, Identifier<Long>, Table> personPersister = MappingEase.mappingBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName)
				// no type of mapping given: neither ordinal nor name
				.addEnum(PersonWithGender::getGender)
				.build(persistenceContext);
		
		Column gender = (Column) personPersister.getMainTable().mapColumnsOnName().get("gender");
		dialect.getJavaTypeToSqlTypeMapping().put(gender, "VARCHAR(255)");
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersonWithGender person = new PersonWithGender(new PersistableIdentifier<>(1L));
		person.setName(null);
		person.setGender(null);
		
		personPersister.insert(person);
	}
	
	@Test
	public void update_nullValues() {
		Persister<PersonWithGender, Identifier<Long>, Table> personPersister = MappingEase.mappingBuilder(PersonWithGender.class, Identifier.LONG_TYPE)
				.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName)
				// no type of mapping given: neither ordinal nor name
				.addEnum(PersonWithGender::getGender)
				.build(persistenceContext);
		
		Column gender = (Column) personPersister.getMainTable().mapColumnsOnName().get("gender");
		dialect.getJavaTypeToSqlTypeMapping().put(gender, "VARCHAR(255)");
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		PersonWithGender person = new PersonWithGender(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.setGender(Gender.MALE);
		
		personPersister.insert(person);
		
		Identifier<Long> id = person.getId();
		System.out.println(id.isPersisted());
		PersonWithGender updatedPerson = new PersonWithGender(id);
		int updatedRowCount = personPersister.update(updatedPerson, person, true);
		assertEquals(1, updatedRowCount);
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
		try {
			MappingEase.mappingBuilder(Country.class, long.class)
					.add(Country::getName).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.embed(Country::getPresident)
					.overrideName(Person::getId, "personId")
					.overrideName(Person::getName, "personName")
					.innerEmbed(Person::getTimestamp)
					.embed(Country::getTimestamp)
					.add(Country::getId)
					.add(Country::setDescription, "zxx")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					.build(persistenceContext);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			Persister<Country, Long, Table> persister = MappingEase.mappingBuilder(Country.class, long.class)
					.add(Country::getName).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.embed(Country::getPresident)
					.innerEmbed(Person::getTimestamp)
					.embed(Country::getTimestamp)
					.add(Country::getId, "zz")
					.addOneToOne(Country::getPresident, MappingEase.mappingBuilder(Person.class, long.class))
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					.add(Country::getDescription, "xx")
					.build(persistenceContext);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			MappingEase.mappingBuilder(Country.class, long.class)
					.add(Country::getName)
					.add(Country::getId, "zz")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					// embed with setter
					.embed(Country::setPresident)
					// inner embed with setter
					.innerEmbed(Person::setTimestamp)
					// embed with setter
					.embed(Country::setTimestamp)
					.addOneToManySet(Country::getCities, MappingEase.mappingBuilder(City.class, long.class))
					.add(Country::getDescription, "xx")
					.add(Country::getDummyProperty, "dd")
					.build(persistenceContext);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
					.add(Person::getName);
			
			MappingEase.mappingBuilder(Country.class, long.class)
					.add(Country::getName)
					.add(Country::getId, "zz")
					.addOneToOne(Country::setPresident, MappingEase.mappingBuilder(Person.class, long.class))
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					// embed with setter
					.embed(Country::getPresident, personMappingBuilder)
					.build(persistenceContext);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			EmbeddedBeanMappingStrategyBuilder<Person> personMappingBuilder = FluentEmbeddableMappingConfigurationSupport.from(Person.class)
					.add(Person::getName);
			
			MappingEase.mappingBuilder(Country.class, long.class)
					.add(Country::getName)
					.add(Country::getId, "zz")
					.embed(Country::getPresident, personMappingBuilder)
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					.addOneToOne(Country::setPresident, MappingEase.mappingBuilder(Person.class, long.class))
					// reusing embeddable ...
					.embed(Country::getPresident, personMappingBuilder)
					// with getter override
					.overrideName(Person::getName, "toto")
					// with setter override
					.overrideName(Person::setName, "tata")
					.build(persistenceContext);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		class PersonTable extends Table<PersonTable> {
			
			Column<PersonTable, Gender> gender = addColumn("gender", Gender.class);
			Column<PersonTable, String> name = addColumn("name", String.class);
			
			PersonTable() {
				super("Person");
			}
		}
		PersonTable personTable = new PersonTable();
		try {
			MappingEase.mappingBuilder(PersonWithGender.class, long.class)
					.add(Person::getName)
					.add(Person::getName, personTable.name)
					.addEnum(PersonWithGender::getGender).byOrdinal()
					.embed(Person::setTimestamp)
					.overrideName(Timestamp::getCreationDate, "myDate")
					.addEnum(PersonWithGender::getGender, "MM").byOrdinal()
					.addEnum(PersonWithGender::getGender, personTable.gender).byOrdinal()
					.add(PersonWithGender::getId, "zz")
					.addEnum(PersonWithGender::setGender).byName()
					.embed(Person::getTimestamp)
					.addEnum(PersonWithGender::setGender, "MM").byName()
					.build(persistenceContext, new Table<>("person"));
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
	}
	
	protected static class Toto implements Identified<UUID> {
		
		private final Identifier<UUID> id;
		
		private Identifier<UUID> identifier;
		
		private String name;
		
		private String firstName;
		
		private Timestamp timestamp;
		
		public Toto() {
			id = new PersistableIdentifier<>(UUID.randomUUID());
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
		
		public Identifier<UUID> getIdentifier() {
			return identifier;
		}
		
		public void setIdentifier(Identifier<UUID> id) {
			this.identifier = id;
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
		public Identifier<UUID> getId() {
			return id;
		}
		
		public void setId(Identifier<UUID> id) {
			
		}
		
		public Timestamp getTimestamp() {
			return timestamp;
		}
		
		public void setTimestamp(Timestamp timestamp) {
			this.timestamp = timestamp;
		}
	}
	
}
