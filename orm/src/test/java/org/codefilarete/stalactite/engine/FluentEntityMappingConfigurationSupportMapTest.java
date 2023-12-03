package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImplTest.ToStringBuilder;
import org.codefilarete.stalactite.engine.model.Car.Radio;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Person.AddressBookType;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
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
	
	private final HSQLDBDialect dialect = new HSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	void initTest() {
		dialect.getDmlGenerator().sortColumnsAlphabetically();	// for steady checks on SQL orders
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getColumnBinderRegistry().register((Class) Identified.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
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
	void withMapEntryTableNaming() {
		MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.withMapEntryTableNaming(new MapEntryTableNamingStrategy() {
					
					@Override
					public String giveTableName(AccessorDefinition accessorDefinition, Class<?> keyType, Class<?> valueType) {
						return "Toto";
					}
					
					@Override
					public <RIGHTTABLE extends Table<RIGHTTABLE>, RIGHTID> Map<Column<RIGHTTABLE, ?>, String> giveMapKeyColumnNames(AccessorDefinition accessorDefinition, Class entityType, PrimaryKey<RIGHTTABLE, RIGHTID> rightPrimaryKey, Set<String> existingColumnNames) {
						return null;
					}
				})
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
		// Changing entry value to check value is also updated
		person.getAddresses().put(new Timestamp(now.minusDays(1), now.minusDays(1)), "Paris");
		person.getAddresses().put(new Timestamp(now.minusDays(5), now.minusDays(5)), "Marseille");
		
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getAddresses()).isEqualTo(Maps.forHashMap(Timestamp.class, String.class)
				.add(new Timestamp(now.minusDays(5), now.minusDays(5)), "Marseille")
				.add(new Timestamp(now.minusDays(1), now.minusDays(1)), "Paris")
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
		// Changing entry value to check value is also updated
		person.getContracts().put("Lyon", new Timestamp(now.minusDays(2), now.minusDays(2)));
		person.getContracts().put("Marseille", new Timestamp(now.minusDays(5), now.minusDays(5)));
		
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getContracts()).isEqualTo(Maps.forHashMap(String.class, Timestamp.class)
				.add("Marseille", new Timestamp(now.minusDays(5), now.minusDays(5)))
				.add("Lyon", new Timestamp(now.minusDays(2), now.minusDays(2)))
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
		// Changing entry value to check value is also updated
		Radio radio4 = new Radio("789");
		radio4.setModel("model4");
		person.getMapPropertyMadeOfComplexTypes().put(new Timestamp(now.minusDays(2), now.minusDays(2)), radio4);
		person.getMapPropertyMadeOfComplexTypes().put(new Timestamp(now.minusDays(3), now.minusDays(3)), radio3);
		
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getMapPropertyMadeOfComplexTypes()).isEqualTo(Maps.forHashMap(Timestamp.class, Radio.class)
				.add(new Timestamp(now.minusDays(2), now.minusDays(2)), radio4)
				.add(new Timestamp(now.minusDays(3), now.minusDays(3)), radio3)
		);
		
		personPersister.delete(loadedPerson);
		Set<String> remainingAddressBook = persistenceContext.newQuery("select 'key' from Person_mapPropertyMadeOfComplexTypes", String.class)
				.mapKey("key", String.class)
				.execute();
		assertThat(remainingAddressBook).isEmpty();
	}

	@Test
	void crud_keyAndValueIsComplexType_overrideColumnName() {
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
	
	
	@Nested
	class KeyIsEntity {
		
		@Test
		void crud() {
			ConfiguredRelationalPersister<Person, Identifier<Long>> personPersister = (ConfiguredRelationalPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapMap(Person::getMapPropertyMadeOfEntityAsKey, Country.class, String.class)
					.withKeyMapping(MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
							.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(Country::getName)
							.map(Country::getDescription)
					)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person person = new Person(new PersistableIdentifier<>(1L));
			person.setMapPropertyMadeOfEntityAsKey(Maps.forHashMap(Country.class, String.class)
					.add(new Country(1), "Grenoble")
					.add(new Country(2), "Lyon")
			);
			
			personPersister.insert(person);
			
			Person loadedPerson = personPersister.select(person.getId());
			assertThat(loadedPerson.getMapPropertyMadeOfEntityAsKey()).isEqualTo(Maps.forHashMap(Country.class, String.class)
					.add(new Country(1), "Grenoble")
					.add(new Country(2), "Lyon")
			);
			
			person.getMapPropertyMadeOfEntityAsKey().remove(new Country(1));
			// Changing entry value to check value is also updated
			person.getMapPropertyMadeOfEntityAsKey().put(new Country(2), "Paris");
			person.getMapPropertyMadeOfEntityAsKey().put(new Country(3), "Marseille");
			
			personPersister.update(person, loadedPerson, true);
			
			loadedPerson = personPersister.select(person.getId());
			assertThat(loadedPerson.getMapPropertyMadeOfEntityAsKey())
					.isEqualTo(Maps.forHashMap(Country.class, String.class)
					.add(new Country(2), "Paris")
					.add(new Country(3), "Marseille")
			);
			
			personPersister.delete(loadedPerson);
			Set<String> remainingEntries = persistenceContext.newQuery("select 'key' from Person_mapPropertyMadeOfEntityAsKey", String.class)
					.mapKey("key", String.class)
					.execute();
			assertThat(remainingEntries).isEmpty();
			
			// by default key entities are not deleted since cascading is not defined
			Set<Long> remainingCountries = persistenceContext.newQuery("select id from Country", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(remainingCountries).containsExactlyInAnyOrder(2L, 3L);
		}
		
		@Test
		void crud_associationOnly() {
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription);
			EntityPersister<Country, Identifier<Long>> countryPersister = countryPersisterConfiguration.build(persistenceContext);
			ConfiguredRelationalPersister<Person, Identifier<Long>> personPersister = (ConfiguredRelationalPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapMap(Person::getMapPropertyMadeOfEntityAsKey, Country.class, String.class)
					.withKeyMapping(countryPersisterConfiguration)
						.cascading(RelationMode.ASSOCIATION_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person person = new Person(new PersistableIdentifier<>(1L));
			Country country1 = new Country(1);
			Country country2 = new Country(2);
			Country country3 = new Country(3);
			countryPersister.insert(Arrays.asList(country1, country2, country3));
			
			person.setMapPropertyMadeOfEntityAsKey(Maps.forHashMap(Country.class, String.class)
					.add(country1, "Grenoble")
					.add(country2, "Lyon")
			);
			
			personPersister.insert(person);
			
			Person loadedPerson = personPersister.select(person.getId());
			
			person.getMapPropertyMadeOfEntityAsKey().remove(country1);
			// Changing entry value to check value is also updated
			person.getMapPropertyMadeOfEntityAsKey().put(country2, "Paris");
			person.getMapPropertyMadeOfEntityAsKey().put(country3, "Marseille");
			
			personPersister.update(person, loadedPerson, true);
			
			loadedPerson = personPersister.select(person.getId());
			Set<Long> remainingEntries = persistenceContext.newQuery("select key from Person_mapPropertyMadeOfEntityAsKey", Long.class)
					.mapKey("key", Long.class)
					.execute();
			assertThat(remainingEntries).containsExactlyInAnyOrder(2L, 3L);
			
			personPersister.delete(loadedPerson);
			remainingEntries = persistenceContext.newQuery("select key from Person_mapPropertyMadeOfEntityAsKey", Long.class)
					.mapKey("key", Long.class)
					.execute();
			assertThat(remainingEntries).isEmpty();
			
			// by default key entities are not deleted since cascading is not defined
			Set<Long> remainingCountries = persistenceContext.newQuery("select id from Country", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(remainingCountries).containsExactlyInAnyOrder(1L, 2L, 3L);
		}
		
		@Test
		void crud_deleteOrphan() {
			ConfiguredRelationalPersister<Person, Identifier<Long>> personPersister = (ConfiguredRelationalPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapMap(Person::getMapPropertyMadeOfEntityAsKey, Country.class, String.class)
					.withKeyMapping(MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
							.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(Country::getName)
							.map(Country::getDescription)
					).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person person = new Person(new PersistableIdentifier<>(1L));
			person.setMapPropertyMadeOfEntityAsKey(Maps.forHashMap(Country.class, String.class)
					.add(new Country(1), "Grenoble")
					.add(new Country(2), "Lyon")
					.add(new Country(3), "Marseille")
			);
			
			personPersister.insert(person);
			
			Person loadedPerson = personPersister.select(person.getId());
			
			person.getMapPropertyMadeOfEntityAsKey().remove(new Country(1));
			// Changing entry value to check value is also updated
			person.getMapPropertyMadeOfEntityAsKey().put(new Country(2), "Paris");
			person.getMapPropertyMadeOfEntityAsKey().put(new Country(3), "Marseille");
			
			personPersister.update(person, loadedPerson, true);
			
			Set<Long> remainingCountries = persistenceContext.newQuery("select id from Country", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(remainingCountries).containsExactlyInAnyOrder(2L, 3L);
			
			loadedPerson = personPersister.select(person.getId());
			
			personPersister.delete(loadedPerson);
			Set<String> remainingEntries = persistenceContext.newQuery("select 'key' from Person_mapPropertyMadeOfEntityAsKey", String.class)
					.mapKey("key", String.class)
					.execute();
			assertThat(remainingEntries).isEmpty();
			
			// by default key entities are not deleted since cascading is not defined
			remainingCountries = persistenceContext.newQuery("select id from Country", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(remainingCountries).isEmpty();
		}
		
		@Test
		void crud_readOnly() throws SQLException {
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription);
			EntityPersister<Country, Identifier<Long>> countryPersister = countryPersisterConfiguration.build(persistenceContext);
			ConfiguredRelationalPersister<Person, Identifier<Long>> personPersister = (ConfiguredRelationalPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapMap(Person::getMapPropertyMadeOfEntityAsKey, Country.class, String.class)
					.withKeyMapping(countryPersisterConfiguration)
					.cascading(RelationMode.READ_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Person person = new Person(new PersistableIdentifier<>(1L));
			Country country1 = new Country(1);
			Country country2 = new Country(2);
			Country country3 = new Country(3);
			countryPersister.insert(Arrays.asList(country1, country2, country3));
			
			person.setMapPropertyMadeOfEntityAsKey(Maps.forHashMap(Country.class, String.class)
					.add(country1, "Grenoble")
					.add(country2, "Lyon")
			);
			
			personPersister.insert(person);
			
			Set<Long> remainingEntries = persistenceContext.newQuery("select key from Person_mapPropertyMadeOfEntityAsKey", Long.class)
					.mapKey("key", Long.class)
					.execute();
			assertThat(remainingEntries).isEmpty();
			
			
			Table associationTable = new Table("Person_mapPropertyMadeOfEntityAsKey");
			Column idColumn = associationTable.addColumn("id", Long.class);
			Column keyColumn = associationTable.addColumn("key", Long.class);
			Column valueColumn = associationTable.addColumn("value", String.class);
			persistenceContext.insert(associationTable)
					.set(idColumn, 1L)
					.set(keyColumn, 1L)
					.set(valueColumn, "Grenoble")
					.execute();
			persistenceContext.insert(associationTable)
					.set(idColumn, 1L)
					.set(keyColumn, 2L)
					.set(valueColumn, "Lyon")
					.execute();
			
			Person loadedPerson = personPersister.select(person.getId());
			
			person.getMapPropertyMadeOfEntityAsKey().remove(country1);
			// Changing entry value to check value is also updated
			person.getMapPropertyMadeOfEntityAsKey().put(country2, "Paris");
			person.getMapPropertyMadeOfEntityAsKey().put(country3, "Marseille");
			
			loadedPerson = personPersister.select(person.getId());
			personPersister.update(person, loadedPerson, true);
			
			remainingEntries = persistenceContext.newQuery("select key from Person_mapPropertyMadeOfEntityAsKey", Long.class)
					.mapKey("key", Long.class)
					.execute();
			// Country 1 & 2 should still be present, and Country 3 missing from association table
			assertThat(remainingEntries).containsExactlyInAnyOrder(1L, 2L);
			
			// We must drop the foreign key between Person table and association table to let personPersister delete
			// the person, else the constraint raises an error because the association table keeps a reference to source one
			persistenceContext.getConnectionProvider().giveConnection()
					.prepareStatement("alter table Person_mapPropertyMadeOfEntityAsKey drop constraint FK_Person_mapPropertyMadeOfEntityAsKey_id_Person_id").execute();
			loadedPerson = personPersister.select(person.getId());
			personPersister.delete(loadedPerson);
			remainingEntries = persistenceContext.newQuery("select key from Person_mapPropertyMadeOfEntityAsKey", Long.class)
					.mapKey("key", Long.class)
					.execute();
			// Country 2 should still be present
			assertThat(remainingEntries).containsExactlyInAnyOrder(1L, 2L);
			
			// by default key entities are not deleted since cascading is not defined
			Set<Long> remainingCountries = persistenceContext.newQuery("select id from Country", Long.class)
					.mapKey("id", Long.class)
					.execute();
			assertThat(remainingCountries).containsExactlyInAnyOrder(1L, 2L, 3L);
		}
		
		@Test
		void foreignKey_creation() {
			ConfiguredRelationalPersister<Person, Identifier<Long>> personPersister = (ConfiguredRelationalPersister<Person, Identifier<Long>>) MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Person::getName)
					.mapMap(Person::getMapPropertyMadeOfEntityAsKey, Country.class, String.class)
					.withKeyMapping(MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
							.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(Country::getName)
							.map(Country::getDescription)
					)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Map<String, Table> tablePerName = map(personPersister.getEntityJoinTree().giveTables(), Table::getName);
			assertThat(tablePerName.keySet()).containsExactlyInAnyOrder("Person", "Person_mapPropertyMadeOfEntityAsKey", "Country");
			Table<?> personTable = tablePerName.get("Person");
			Table<?> countryTable = tablePerName.get("Country");
			Table<?> mapTable = tablePerName.get("Person_mapPropertyMadeOfEntityAsKey");
			
			Function<Column, String> columnPrinter = ToStringBuilder.of(", ",
					Column::getAbsoluteName,
					chain(Column::getJavaType, Reflections::toString));
			Function<ForeignKey, String> fkPrinter = ToStringBuilder.of(", ",
					ForeignKey::getName,
					link(ForeignKey::getColumns, ToStringBuilder.asSeveral(columnPrinter)),
					link(ForeignKey::getTargetColumns, ToStringBuilder.asSeveral(columnPrinter)));
			
			assertThat(mapTable.getForeignKeys())
					.usingElementComparator(Comparator.comparing(fkPrinter))
					.containsExactlyInAnyOrder(
							new ForeignKey("FK_Person_mapPropertyMadeOfEntityAsKey_id_Person_id", mapTable.getColumn("id"), personTable.getColumn("id")),
							new ForeignKey("FK_Person_mapPropertyMadeOfEntityAsKey_key_Country_id", mapTable.getColumn("key"), countryTable.getColumn("id")));
		}
	}
}