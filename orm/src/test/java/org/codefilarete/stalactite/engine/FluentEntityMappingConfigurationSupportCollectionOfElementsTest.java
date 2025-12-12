package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;

import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.engine.FluentEntityMappingConfigurationSupportTest.State;
import org.codefilarete.stalactite.engine.FluentEntityMappingConfigurationSupportTest.Toto;
import org.codefilarete.stalactite.engine.configurer.ToStringBuilder;
import org.codefilarete.stalactite.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Truck;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.MappingEase.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.dsl.MappingEase.subentityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.tool.collection.Iterables.map;
import static org.codefilarete.tool.function.Functions.chain;
import static org.codefilarete.tool.function.Functions.link;

class FluentEntityMappingConfigurationSupportCollectionOfElementsTest {
	
	private static final Class<Identifier<UUID>> UUID_TYPE = (Class) Identifier.class;
	
	// NB: dialect is made non static because we register binder for the same column several times in these tests
	// and this is not supported : the very first one takes priority  
	private final Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	void initTest() {
		dialect.getDmlGenerator().sortColumnsAlphabetically();	// for steady checks on SQL orders
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		
		persistenceContext = new PersistenceContext(dataSource, dialect);
	}
	
	@Test
	void insert() {
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.initNicknames();
		person.addNickname("tonton");
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getNicknames()).isEqualTo(Arrays.asSet("tonton"));
	}
	
	@Test
	void update_withNewObject() {
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.initNicknames();
		person.addNickname("tonton");
		person.addNickname("tintin");
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getNicknames()).isEqualTo(Arrays.asSet("tintin", "tonton"));
		
		
		loadedPerson.addNickname("toutou");
		personPersister.update(loadedPerson, person, true);
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getNicknames()).isEqualTo(Arrays.asSet("tintin", "tonton", "toutou"));
	}
	
	@Test
	void update_objectRemoval() {
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.initNicknames();
		person.addNickname("tonton");
		person.addNickname("tintin");
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		
		person.getNicknames().remove("tintin");
		personPersister.update(person, loadedPerson, true);
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getNicknames()).isEqualTo(Arrays.asSet("tonton"));
	}
	
	@Test
	void delete() {
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.initNicknames();
		person.addNickname("tonton");
		person.addNickname("tintin");
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		personPersister.delete(loadedPerson);
		Set<String> remainingNickNames = persistenceContext.newQuery("select nickNames from Person_nicknames", String.class)
				.mapKey("nickNames", String.class)
				.execute(Accumulators.toSet());
		assertThat(remainingNickNames).isEmpty();
	}
	
	@Test
	void withCollectionFactory() {
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
				.withCollectionFactory(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER.reversed()))
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setName("toto");
		person.initNicknames();
		person.addNickname("d");
		person.addNickname("a");
		person.addNickname("c");
		person.addNickname("b");
		
		// because nickNames is initialized with HashSet we get this order
		assertThat(person.getNicknames()).isEqualTo(Arrays.asSet("a", "b", "c", "d"));
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		// because nickNames is initialized with TreeSet with reversed order we get this order
		assertThat(loadedPerson.getNicknames()).isEqualTo(Arrays.asSet("d", "c", "b", "a"));
	}
	
	@Test
	void foreignKeyIsPresent() {
		entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
				.build(persistenceContext);
		
		Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table<?>> tablePerName = map(tables, Table::getName);
		Table<?> personTable = tablePerName.get("Person");
		Table<?> nickNamesTable = tablePerName.get("Person_nicknames");
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, Reflections::toString));
		Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
				ForeignKey::getName,
				link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
				link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
		
		assertThat(nickNamesTable.getForeignKeys())
				.usingElementComparator(Comparator.comparing(fkPrinter))
				.containsExactly(new ForeignKey("FK_Person_nicknames_id_Person_id", nickNamesTable.getColumn("id"), personTable.getColumn("id")));
	}
	
	
	@Test
	void mappedBy() {
		entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
				.reverseJoinColumn("identifier")
				.build(persistenceContext);
		
		Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table<?>> tablePerName = map(tables, Table::getName);
		Table<?> personTable = tablePerName.get("Person");
		Table<?> nickNamesTable = tablePerName.get("Person_nicknames");
		assertThat(nickNamesTable).isNotNull();
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, Reflections::toString));
		Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
				ForeignKey::getName,
				link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
				link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
		
		assertThat(nickNamesTable.getForeignKeys())
				.usingElementComparator(Comparator.comparing(fkPrinter))
				.containsExactly(new ForeignKey("FK_Person_nicknames_identifier_Person_id", nickNamesTable.getColumn("identifier"), personTable.getColumn("id")));
	}
	
	@Test
	void withElementCollectionTableNaming() {
		entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.withElementCollectionTableNaming(accessorDefinition -> "Toto")
				.mapCollection(Person::getNicknames, String.class)
				.build(persistenceContext);
		
		Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table<?>> tablePerName = map(tables, Table::getName);
		Table<?> personTable = tablePerName.get("Person");
		Table<?> nickNamesTable = tablePerName.get("Toto");
		assertThat(nickNamesTable).isNotNull();
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, Reflections::toString));
		Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
				ForeignKey::getName,
				link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
				link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
		
		assertThat(nickNamesTable.getForeignKeys())
				.usingElementComparator(Comparator.comparing(fkPrinter))
				.containsExactly(new ForeignKey("FK_Toto_id_Person_id", nickNamesTable.getColumn("id"), personTable.getColumn("id")));
	}
	
	@Test
	void withTable() {
		Table nickNamesTable = new Table("Toto");
		
		entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
				.onTable(nickNamesTable)
				.build(persistenceContext);
		
		Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table<?>> tablePerName = map(tables, Table::getName);
		Table<?> personTable = tablePerName.get("Person");
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, Reflections::toString));
		Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
				ForeignKey::getName,
				link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
				link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
		
		assertThat(nickNamesTable.getForeignKeys())
				.usingElementComparator(Comparator.comparing(fkPrinter))
				.containsExactly(new ForeignKey("FK_Toto_id_Person_id", nickNamesTable.getColumn("id"), personTable.getColumn("id")));
	}
	
	@Test
	void withTableName() {
		entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
					.onTable("Toto")
				.build(persistenceContext);
		
		Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table<?>> tablePerName = map(tables, Table::getName);
		Table<?> personTable = tablePerName.get("Person");
		Table<?> nickNamesTable = tablePerName.get("Toto");
		assertThat(nickNamesTable).isNotNull();
		
		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, Reflections::toString));
		Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
				ForeignKey::getName,
				link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
				link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
		
		assertThat(nickNamesTable.getForeignKeys())
				.usingElementComparator(Comparator.comparing(fkPrinter))
				.containsExactly(new ForeignKey("FK_Toto_id_Person_id", nickNamesTable.getColumn("id"), personTable.getColumn("id")));
	}
	
	@Test
	void changeColumnName() {
		entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
					.elementColumnName("toto")
				.build(persistenceContext);
		
		Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table<?>> tablePerName = map(tables, Table::getName);
		Table<?> nickNamesTable = tablePerName.get("Person_nicknames");
		
		assertThat(nickNamesTable.mapColumnsOnName().keySet()).containsExactlyInAnyOrder("id", "toto");
	}
	
	@Test
	void changeColumnSize() {
		entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
					.elementColumnSize(Size.length(36))
				.build(persistenceContext);
		
		Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table<?>> tablePerName = map(tables, Table::getName);
		Table<?> nickNamesTable = tablePerName.get("Person_nicknames");
		
		assertThat(nickNamesTable.mapColumnsOnName().get("nicknames").getSize())
				.usingRecursiveComparison()
				.isEqualTo(Size.length(36));
	}
	
	@Test
	void read_deepInTree() {
		EntityPersister<Country, Identifier<Long>> countryPersister = entityBuilder(Country.class, LONG_TYPE)
				.mapKey(Country::getId, IdentifierPolicy.<Country, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
				.map(Country::getName)
				.map(Country::getDescription)
				.mapOneToOne(Country::getPresident, entityBuilder(Person.class, LONG_TYPE)
						.mapKey(Person::getId, IdentifierPolicy.<Person, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
						.map(Person::getName)
						.mapCollection(Person::getNicknames, String.class)
				)
				.mapOneToMany(Country::getCities, entityBuilder(City.class, LONG_TYPE)
						.mapKey(City::getId, IdentifierPolicy.<City, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
						.map(City::getName))
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Country country = new Country(42);
		country.setName("Toto");
		country.addCity(new City(111));
		country.addCity(new City(222));
		Person president = new Person(666);
		president.setName("me");
		president.initNicknames();
		president.addNickname("John Do");
		president.addNickname("Jane Do");
		country.setPresident(president);
		
		countryPersister.insert(java.util.Arrays.asList(country));
		
		Country loadedCountry = countryPersister.select(country.getId());
		assertThat(loadedCountry.getPresident().getNicknames()).containsExactlyInAnyOrderElementsOf(country.getPresident().getNicknames());
	}
	
	
	@Test
	void crudEnum() {
		Table totoTable = new Table("Toto");
		Column idColumn = totoTable.addColumn("id", UUID_TYPE);
		dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
		
		ConfiguredPersister<Toto, Identifier<UUID>> personPersister = (ConfiguredPersister<Toto, Identifier<UUID>>) entityBuilder(Toto.class, UUID_TYPE)
				.onTable(totoTable)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName)
				.mapCollection(Toto::getPossibleStates, State.class)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Toto person = new Toto();
		person.setName("toto");
		person.getPossibleStates().add(State.DONE);
		person.getPossibleStates().add(State.IN_PROGRESS);
		
		personPersister.insert(person);
		
		Toto loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getPossibleStates()).isEqualTo(Arrays.asSet(State.DONE, State.IN_PROGRESS));
	}
	
	@Test
	void crudComplexType() {
		Table totoTable = new Table("Toto");
		Column idColumn = totoTable.addColumn("id", UUID_TYPE);
		dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
		
		ConfiguredPersister<Toto, Identifier<UUID>> personPersister = (ConfiguredPersister<Toto, Identifier<UUID>>) entityBuilder(Toto.class, UUID_TYPE)
				.onTable(totoTable)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName)
				.mapCollection(Toto::getTimes, Timestamp.class, embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Toto person = new Toto();
		person.setName("toto");
		Timestamp timestamp1 = new Timestamp(LocalDateTime.now().plusDays(1), LocalDateTime.now().plusDays(1));
		Timestamp timestamp2 = new Timestamp(LocalDateTime.now().plusDays(2), LocalDateTime.now().plusDays(2));
		person.getTimes().add(timestamp1);
		person.getTimes().add(timestamp2);
		
		personPersister.insert(person);
		
		Toto loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getTimes()).isEqualTo(Arrays.asSet(timestamp1, timestamp2));
	}
	
	@Test
	void crudComplexType_overrideColumnName() {
		Table totoTable = new Table("toto");
		Column idColumn = totoTable.addColumn("id", UUID_TYPE);
		dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
		
		ConfiguredPersister<Toto, Identifier<UUID>> personPersister = (ConfiguredPersister<Toto, Identifier<UUID>>) entityBuilder(Toto.class, UUID_TYPE)
				.onTable(totoTable)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName)
				.mapCollection(Toto::getTimes, Timestamp.class, embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
				.overrideName(Timestamp::getCreationDate, "createdAt")
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table<?>> tablePerName = map(tables, Table::getName);
		Table<?> totoTimesTable = tablePerName.get("Toto_times");
		Map<String, ? extends Column<?, ?>> timesTableColumn = totoTimesTable.mapColumnsOnName();
		assertThat(timesTableColumn.get("createdAt")).isNotNull();
		
		Toto person = new Toto();
		person.setName("toto");
		Timestamp timestamp1 = new Timestamp(LocalDateTime.now().plusDays(1), LocalDateTime.now().plusDays(1));
		Timestamp timestamp2 = new Timestamp(LocalDateTime.now().plusDays(2), LocalDateTime.now().plusDays(2));
		person.getTimes().add(timestamp1);
		person.getTimes().add(timestamp2);
		
		personPersister.insert(person);
		
		Toto loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getTimes()).isEqualTo(Arrays.asSet(timestamp1, timestamp2));
	}
	
	@Nested
	class OnSubEntity {
		
		@Test
		void crudComplexType_overrideColumnName() {
			
			dialect.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
			dialect.getSqlTypeRegistry().put(Color.class, "int");
			
			entityBuilder(AbstractVehicle.class, LONG_TYPE)
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
							.addSubClass(subentityBuilder(Car.class)
											.map(Car::getId)
											.map(Car::getModel)
											.mapCollection(Car::getPlates, String.class)
												.elementColumnName("toto")
											.mapCollection(Car::getInspections, Timestamp.class, embeddableBuilder(Timestamp.class)
													.map(Timestamp::getCreationDate).columnName("createdAt")    // this column will be overridden to "inspectionDate"
													.map(Timestamp::getModificationDate).columnName("modifiedAt"))
												.overrideName(Timestamp::getCreationDate, "inspectionDate")
												.overrideSize(Timestamp::getCreationDate, Size.length(36))
									, "CAR")
							.addSubClass(subentityBuilder(Truck.class)
									.map(Truck::getId)
									.map(Truck::getColor), "TRUCK"))
					.build(persistenceContext);
			
			Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
			Map<String, Table<?>> tablePerName = map(tables, Table::getName);
			Table<?> platesTable = tablePerName.get("Car_plates");
			assertThat(platesTable.mapColumnsOnName().keySet()).containsExactlyInAnyOrder("id", "toto");
			Table<?> inspectionsTable = tablePerName.get("Car_inspections");
			assertThat(inspectionsTable.mapColumnsOnName().keySet()).containsExactlyInAnyOrder("id", "modifiedAt", "inspectionDate");
			assertThat(inspectionsTable.mapColumnsOnName().get("inspectionDate").getSize()).usingRecursiveComparison().isEqualTo(Size.length(36));
		}
	}
}
