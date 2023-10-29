package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImplTest.ToStringBuilder;
import org.codefilarete.stalactite.engine.model.Car.Radio;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Person.AddressBookType;
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
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.tool.collection.Iterables.map;
import static org.codefilarete.tool.function.Functions.chain;
import static org.codefilarete.tool.function.Functions.link;

@Nested
class FluentEntityMappingConfigurationSupportMapTest {
	
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
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setPhoneNumbers(Maps.forHashMap(String.class, String.class)
				.add("home", "01 11 11 11 11")
				.add("work", "02 22 22 22 22")
				.add("mobile", "03 33 33 33 33")
		);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getPhoneNumbers()).isEqualTo(Maps.forHashMap(String.class, String.class)
				.add("home", "01 11 11 11 11")
				.add("work", "02 22 22 22 22")
				.add("mobile", "03 33 33 33 33"));
	}
	
	@Test
	void update_objectAddition() {
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
				.build(persistenceContext);

		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setPhoneNumbers(Maps.forHashMap(String.class, String.class)
				.add("home", "01 11 11 11 11")
				.add("work", "02 22 22 22 22")
				.add("mobile", "03 33 33 33 33")
		);

		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getPhoneNumbers()).isEqualTo(Maps.forHashMap(String.class, String.class)
				.add("home", "01 11 11 11 11")
				.add("work", "02 22 22 22 22")
				.add("mobile", "03 33 33 33 33"));
		
		
		loadedPerson.getPhoneNumbers().put("vacation site", "04 44 44 44 44");
		personPersister.update(loadedPerson, person, true);
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getPhoneNumbers()).isEqualTo(Maps.forHashMap(String.class, String.class)
				.add("home", "01 11 11 11 11")
				.add("work", "02 22 22 22 22")
				.add("mobile", "03 33 33 33 33")
				.add("vacation site", "04 44 44 44 44")
		);
	}

	@Test
	void update_objectRemoval() {
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setPhoneNumbers(Maps.forHashMap(String.class, String.class)
				.add("home", "01 11 11 11 11")
				.add("work", "02 22 22 22 22")
				.add("mobile", "03 33 33 33 33")
		);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getPhoneNumbers()).isEqualTo(Maps.forHashMap(String.class, String.class)
				.add("home", "01 11 11 11 11")
				.add("work", "02 22 22 22 22")
				.add("mobile", "03 33 33 33 33"));
		
		
		loadedPerson.getPhoneNumbers().remove("home");
		personPersister.update(loadedPerson, person, true);
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getPhoneNumbers()).isEqualTo(Maps.forHashMap(String.class, String.class)
				.add("work", "02 22 22 22 22")
				.add("mobile", "03 33 33 33 33")
		);
	}

	@Test
	void delete() {
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
				.mapCollection(Person::getNicknames, String.class)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setPhoneNumbers(Maps.forHashMap(String.class, String.class)
				.add("home", "01 11 11 11 11")
				.add("work", "02 22 22 22 22")
				.add("mobile", "03 33 33 33 33")
		);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getPhoneNumbers()).isEqualTo(Maps.forHashMap(String.class, String.class)
				.add("home", "01 11 11 11 11")
				.add("work", "02 22 22 22 22")
				.add("mobile", "03 33 33 33 33"));
		
		personPersister.delete(loadedPerson);
		Set<String> remainingPhoneNumbers = persistenceContext.newQuery("select 'key' from Person_phoneNumbers", String.class)
				.mapKey("key", String.class)
				.execute();
		assertThat(remainingPhoneNumbers).isEmpty();
	}

	@Test
	void withCollectionFactory() {
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
				.withMapFactory(() -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER.reversed()))
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setPhoneNumbers(Maps.forHashMap(String.class, String.class)
				.add("home", "01 11 11 11 11")
				.add("work", "02 22 22 22 22")
				.add("mobile", "03 33 33 33 33")
		);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getPhoneNumbers()).containsExactly(
				new SimpleEntry<>("work", "02 22 22 22 22"),
				new SimpleEntry<>("mobile", "03 33 33 33 33"),
				new SimpleEntry<>("home", "01 11 11 11 11")
		);
	}

	@Test
	void foreignKeyIsPresent() {
		MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
				.build(persistenceContext);

		Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table> tablePerName = map(tables, Table::getName);
		Table personTable = tablePerName.get("Person");
		Table nickNamesTable = tablePerName.get("Person_phoneNumbers");

		Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
				Column::getAbsoluteName,
				chain(Column::getJavaType, Reflections::toString));
		Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
				ForeignKey::getName,
				link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
				link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));

		assertThat(nickNamesTable.getForeignKeys())
				.usingElementComparator(Comparator.comparing(fkPrinter))
				.containsExactly(new ForeignKey("FK_Person_phoneNumbers_id_Person_id", nickNamesTable.getColumn("id"), personTable.getColumn("id")));
	}

	@Test
	void withReverseJoinColumn() {
		MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
				.withReverseJoinColumn("identifier")
				.build(persistenceContext);

		Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table> tablePerName = map(tables, Table::getName);
		Table personTable = tablePerName.get("Person");
		Table nickNamesTable = tablePerName.get("Person_phoneNumbers");
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
				.containsExactly(new ForeignKey("FK_Person_phoneNumbers_identifier_Person_id", nickNamesTable.getColumn("identifier"), personTable.getColumn("id")));
	}

	@Test
	void withElementCollectionTableNaming() {
		MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.withElementCollectionTableNaming(accessorDefinition -> "Toto")
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
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
		Table phoneNumbersTable = new Table("Toto");

		MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
				.withTable(phoneNumbersTable)
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

		assertThat(phoneNumbersTable.getForeignKeys())
				.usingElementComparator(Comparator.comparing(fkPrinter))
				.containsExactly(new ForeignKey("FK_Toto_id_Person_id", phoneNumbersTable.getColumn("id"), personTable.getColumn("id")));
	}

	@Test
	void withTableName() {
		MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
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
	void overrideKeyColumnName() {
		MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
				.withKeyColumn("toto")
				.build(persistenceContext);

		Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table> tablePerName = map(tables, Table::getName);
		Table nickNamesTable = tablePerName.get("Person_phoneNumbers");

		assertThat(nickNamesTable.mapColumnsOnName().keySet()).containsExactlyInAnyOrder("id", "toto", "value");
	}


	@Test
	void overrideValueColumnName() {
		MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
				.withValueColumn("toto")
				.build(persistenceContext);

		Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
		Map<String, Table> tablePerName = map(tables, Table::getName);
		Table nickNamesTable = tablePerName.get("Person_phoneNumbers");

		assertThat(nickNamesTable.mapColumnsOnName().keySet()).containsExactlyInAnyOrder("id", "toto", "key");
	}

	@Test
	void crudEnum() {
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getAddressBook, AddressBookType.class, String.class)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setAddressBook(Maps.forHashMap(AddressBookType.class, String.class)
				.add(AddressBookType.HOME, "Grenoble")
				.add(AddressBookType.BILLING_ADDRESS, "Lyon")
		);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getAddressBook()).isEqualTo(Maps.forHashMap(AddressBookType.class, String.class)
				.add(AddressBookType.HOME, "Grenoble")
				.add(AddressBookType.BILLING_ADDRESS, "Lyon")
		);
		
		person.getAddressBook().remove(AddressBookType.HOME);
		person.getAddressBook().put(AddressBookType.OTHER, "Marseille");
		
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getAddressBook()).isEqualTo(Maps.forHashMap(AddressBookType.class, String.class)
				.add(AddressBookType.OTHER, "Marseille")
				.add(AddressBookType.BILLING_ADDRESS, "Lyon")
		);
		
		personPersister.delete(loadedPerson);
		Set<String> remainingAddressBook = persistenceContext.newQuery("select 'key' from Person_addressBook", String.class)
				.mapKey("key", String.class)
				.execute();
		assertThat(remainingAddressBook).isEmpty();
	}

	@Test
	void crud_keyIsComplexType() {
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getAddresses, Timestamp.class, String.class)
				.withKeyMapping(MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate)
				)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		LocalDateTime now = LocalDateTime.now();
		person.setAddresses(Maps.forHashMap(Timestamp.class, String.class)
				.add(new Timestamp(now.minusDays(10), now.minusDays(10)), "Grenoble")
				.add(new Timestamp(now.minusDays(1), now.minusDays(1)), "Lyon")
		);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getAddresses()).isEqualTo(Maps.forHashMap(Timestamp.class, String.class)
				.add(new Timestamp(now.minusDays(10), now.minusDays(10)), "Grenoble")
				.add(new Timestamp(now.minusDays(1), now.minusDays(1)), "Lyon")
		);
		
		person.getAddresses().remove(new Timestamp(now.minusDays(10), now.minusDays(10)));
		person.getAddresses().put(new Timestamp(now.minusDays(5), now.minusDays(5)), "Marseille");
		
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getAddresses()).isEqualTo(Maps.forHashMap(Timestamp.class, String.class)
				.add(new Timestamp(now.minusDays(5), now.minusDays(5)), "Marseille")
				.add(new Timestamp(now.minusDays(1), now.minusDays(1)), "Lyon")
		);
		
		personPersister.delete(loadedPerson);
		Set<String> remainingAddressBook = persistenceContext.newQuery("select 'key' from Person_addresses", String.class)
				.mapKey("key", String.class)
				.execute();
		assertThat(remainingAddressBook).isEmpty();
	}

	@Test
	void crud_valueIsComplexType() {
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getContracts, String.class, Timestamp.class)
				.withValueMapping(MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate)
				)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		LocalDateTime now = LocalDateTime.now();
		person.setContracts(Maps.forHashMap(String.class, Timestamp.class)
				.add("Grenoble", new Timestamp(now.minusDays(10), now.minusDays(10)))
				.add("Lyon", new Timestamp(now.minusDays(1), now.minusDays(1)))
		);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getContracts()).isEqualTo(Maps.forHashMap(String.class, Timestamp.class)
				.add("Grenoble", new Timestamp(now.minusDays(10), now.minusDays(10)))
				.add("Lyon", new Timestamp(now.minusDays(1), now.minusDays(1)))
		);
		
		person.getContracts().remove("Grenoble");
		person.getContracts().put("Marseille", new Timestamp(now.minusDays(5), now.minusDays(5)));
		
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getContracts()).isEqualTo(Maps.forHashMap(String.class, Timestamp.class)
				.add("Marseille", new Timestamp(now.minusDays(5), now.minusDays(5)))
				.add("Lyon", new Timestamp(now.minusDays(1), now.minusDays(1)))
		);
		
		personPersister.delete(loadedPerson);
		Set<String> remainingAddressBook = persistenceContext.newQuery("select 'key' from Person_contracts", String.class)
				.mapKey("key", String.class)
				.execute();
		assertThat(remainingAddressBook).isEmpty();
	}
	
	@Test
	void crud_keyAndValueIsComplexType() {
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getMapPropertyMadeOfComplexTypes, Timestamp.class, Radio.class)
				.withKeyMapping(MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate)
				)
				.withValueMapping(MappingEase.embeddableBuilder(Radio.class)
						.map(Radio::getSerialNumber)
						.map(Radio::getModel)
				)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		LocalDateTime now = LocalDateTime.now();
		Radio radio1 = new Radio("123");
		radio1.setModel("model1");
		Radio radio2 = new Radio("456");
		radio2.setModel("model2");
		person.setMapPropertyMadeOfComplexTypes(Maps.forHashMap(Timestamp.class, Radio.class)
				.add(new Timestamp(now.minusDays(1), now.minusDays(1)), radio1)
				.add(new Timestamp(now.minusDays(2), now.minusDays(2)), radio2)
		);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getMapPropertyMadeOfComplexTypes()).isEqualTo(Maps.forHashMap(Timestamp.class, Radio.class)
				.add(new Timestamp(now.minusDays(1), now.minusDays(1)), radio1)
				.add(new Timestamp(now.minusDays(2), now.minusDays(2)), radio2)
		);
		
		Radio radio3 = new Radio("789");
		radio3.setModel("model3");
		person.getMapPropertyMadeOfComplexTypes().remove(new Timestamp(now.minusDays(1), now.minusDays(1)));
		person.getMapPropertyMadeOfComplexTypes().put(new Timestamp(now.minusDays(3), now.minusDays(3)), radio3);
		
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getMapPropertyMadeOfComplexTypes()).isEqualTo(Maps.forHashMap(Timestamp.class, Radio.class)
				.add(new Timestamp(now.minusDays(2), now.minusDays(2)), radio2)
				.add(new Timestamp(now.minusDays(3), now.minusDays(3)), radio3)
		);
		
		personPersister.delete(loadedPerson);
		Set<String> remainingAddressBook = persistenceContext.newQuery("select 'key' from Person_mapPropertyMadeOfComplexTypes", String.class)
				.mapKey("key", String.class)
				.execute();
		assertThat(remainingAddressBook).isEmpty();
	}

	@Test
	void crudComplexType_overrideColumnName() {
		ConfiguredPersister<Person, Identifier<Long>> personPersister = (ConfiguredPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getMapPropertyMadeOfComplexTypesWithColumnDuplicates, Timestamp.class, Timestamp.class)
				.withKeyMapping(MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate)
				)
				.withValueMapping(MappingEase.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate)
				)
				.overrideKeyColumnName(Timestamp::getCreationDate, "key_creationDate")
				.overrideValueColumnName(Timestamp::getModificationDate, "value_modificationDate")
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		LocalDateTime now = LocalDateTime.now();
		person.setMapPropertyMadeOfComplexTypesWithColumnDuplicates(Maps.forHashMap(Timestamp.class, Timestamp.class)
				.add(new Timestamp(now.minusDays(1), now.minusDays(1)), new Timestamp(now.minusDays(10), now.minusDays(10)))
				.add(new Timestamp(now.minusDays(2), now.minusDays(2)), new Timestamp(now.minusDays(20), now.minusDays(20)))
		);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getMapPropertyMadeOfComplexTypesWithColumnDuplicates()).isEqualTo(Maps.forHashMap(Timestamp.class, Timestamp.class)
				.add(new Timestamp(now.minusDays(1), now.minusDays(1)), new Timestamp(now.minusDays(10), now.minusDays(10)))
				.add(new Timestamp(now.minusDays(2), now.minusDays(2)), new Timestamp(now.minusDays(20), now.minusDays(20)))
		);
		
		person.getMapPropertyMadeOfComplexTypesWithColumnDuplicates().remove(new Timestamp(now.minusDays(1), now.minusDays(1)));
		person.getMapPropertyMadeOfComplexTypesWithColumnDuplicates().put(new Timestamp(now.minusDays(3), now.minusDays(3)), new Timestamp(now.minusDays(30), now.minusDays(30)));
		
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getMapPropertyMadeOfComplexTypesWithColumnDuplicates()).isEqualTo(Maps.forHashMap(Timestamp.class, Timestamp.class)
				.add(new Timestamp(now.minusDays(2), now.minusDays(2)), new Timestamp(now.minusDays(20), now.minusDays(20)))
				.add(new Timestamp(now.minusDays(3), now.minusDays(3)), new Timestamp(now.minusDays(30), now.minusDays(30)))
		);
		
		personPersister.delete(loadedPerson);
		Set<String> remainingAddressBook = persistenceContext.newQuery("select 'key' from Person_mapPropertyMadeOfComplexTypesWithColumnDuplicates", String.class)
				.mapKey("key", String.class)
				.execute();
		assertThat(remainingAddressBook).isEmpty();
	}
}
