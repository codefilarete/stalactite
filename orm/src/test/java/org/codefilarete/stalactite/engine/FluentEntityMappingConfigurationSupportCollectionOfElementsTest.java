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

import org.codefilarete.stalactite.engine.FluentEntityMappingConfigurationSupportTest.State;
import org.codefilarete.stalactite.engine.FluentEntityMappingConfigurationSupportTest.Toto;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImplTest.ToStringBuilder;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.tool.collection.Iterables.map;
import static org.codefilarete.tool.function.Functions.chain;
import static org.codefilarete.tool.function.Functions.link;

@Nested
class FluentEntityMappingConfigurationSupportCollectionOfElementsTest {
	
	private static final Class<Identifier<UUID>> UUID_TYPE = (Class) Identifier.class;
	
	// NB: dialect is made non static because we register binder for the same column several times in these tests
	// and this is not supported : the very first one takes priority  
	private final HSQLDBDialect dialect = new HSQLDBDialect();
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
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
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
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
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
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
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
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
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
				.execute();
		assertThat(remainingNickNames).isEmpty();
	}
	
	@Test
	void withCollectionFactory() {
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
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
		MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
				.build(persistenceContext);
		
		Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table> tablePerName = map(tables, Table::getName);
		Table personTable = tablePerName.get("Person");
		Table nickNamesTable = tablePerName.get("Person_nicknames");
		
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
		MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
				.withReverseJoinColumn("identifier")
				.build(persistenceContext);
		
		Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table> tablePerName = map(tables, Table::getName);
		Table personTable = tablePerName.get("Person");
		Table nickNamesTable = tablePerName.get("Person_nicknames");
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
		MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.withElementCollectionTableNaming(accessorDefinition -> "Toto")
				.mapCollection(Person::getNicknames, String.class)
				.build(persistenceContext);
		
		Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table> tablePerName = map(tables, Table::getName);
		Table personTable = tablePerName.get("Person");
		Table nickNamesTable = tablePerName.get("Toto");
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
		
		MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
				.withTable(nickNamesTable)
				.build(persistenceContext);
		
		Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table> tablePerName = map(tables, Table::getName);
		Table personTable = tablePerName.get("Person");
		
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
		MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
				.withTable("Toto")
				.build(persistenceContext);
		
		Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table> tablePerName = map(tables, Table::getName);
		Table personTable = tablePerName.get("Person");
		Table nickNamesTable = tablePerName.get("Toto");
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
	void overrideColumnName() {
		MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapCollection(Person::getNicknames, String.class)
				.override("toto")
				.build(persistenceContext);
		
		Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table> tablePerName = map(tables, Table::getName);
		Table personTable = tablePerName.get("Person");
		Table nickNamesTable = tablePerName.get("Person_nicknames");
		
		assertThat(nickNamesTable.mapColumnsOnName().keySet()).containsExactlyInAnyOrder("id", "toto");
	}
	
	
	@Test
	void crudEnum() {
		Table totoTable = new Table("Toto");
		Column idColumn = totoTable.addColumn("id", UUID_TYPE);
		dialect.getColumnBinderRegistry().register(idColumn, Identifier.identifierBinder(DefaultParameterBinders.UUID_BINDER));
		dialect.getSqlTypeRegistry().put(idColumn, "VARCHAR(255)");
		
		ConfiguredPersister<Toto, Identifier<UUID>> personPersister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
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
		
		ConfiguredPersister<Toto, Identifier<UUID>> personPersister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName)
				.mapCollection(Toto::getTimes, Timestamp.class, MappingEase.embeddableBuilder(Timestamp.class)
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
		
		ConfiguredPersister<Toto, Identifier<UUID>> personPersister = (ConfiguredPersister<Toto, Identifier<UUID>>) MappingEase.entityBuilder(Toto.class, UUID_TYPE, totoTable)
				.mapKey(Toto::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.UUID_ALREADY_ASSIGNED)
				.map(Toto::getName)
				.mapCollection(Toto::getTimes, Timestamp.class, MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
				.overrideName(Timestamp::getCreationDate, "createdAt")
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table> tablePerName = map(tables, Table::getName);
		Table totoTimesTable = tablePerName.get("Toto_times");
		Map<String, Column> timesTableColumn = totoTimesTable.mapColumnsOnName();
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
}
