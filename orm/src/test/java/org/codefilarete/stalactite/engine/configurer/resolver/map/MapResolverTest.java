package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;

import org.codefilarete.stalactite.dsl.FluentMappings;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.ExecutableQuery;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.compositekey.House;
import org.codefilarete.stalactite.engine.model.compositekey.House.HouseId;
import org.codefilarete.stalactite.engine.model.compositekey.Person.PersonId;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.h2.H2DialectBuilder;
import org.codefilarete.stalactite.sql.h2.test.H2InMemoryDataSource;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.compositeKeyBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;
import static org.codefilarete.tool.collection.Iterables.map;

class MapResolverTest {
	
	// We use H2 because it supports foreign key creation separately from table creation (not SQLite) and tupled-in (not SQLite)
	private final Dialect dialect = H2DialectBuilder.defaultH2Dialect();
	private final DataSource dataSource = new H2InMemoryDataSource();
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	void setUp() {
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		persistenceContext = new PersistenceContext(dataSource, dialect);
	}
	
	@Test
	void crud_scalarMap() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getPhoneNumbers, String.class, String.class);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setName("john");
		Map<String, String> phoneNumbers = new LinkedHashMap<>();
		phoneNumbers.put("home", "01 11 11 11 11");
		phoneNumbers.put("mobile", "03 33 33 33 33");
		person.setPhoneNumbers(phoneNumbers);
		personPersister.insert(person);
		
		Person loaded = personPersister.select(person.getId());
		assertThat(loaded.getPhoneNumbers())
				.containsEntry("home", "01 11 11 11 11")
				.containsEntry("mobile", "03 33 33 33 33");
		
		loaded.getPhoneNumbers().remove("home");
		loaded.getPhoneNumbers().put("office", "02 22 22 22 22");
		personPersister.update(loaded, person, true);
		
		Person reloaded = personPersister.select(person.getId());
		assertThat(reloaded.getPhoneNumbers())
				.containsOnlyKeys("mobile", "office")
				.containsEntry("office", "02 22 22 22 22");
		
		personPersister.delete(reloaded);
		
		Long mapRowCount = persistenceContext.newQuery("select count(*) as cnt from Person_phoneNumbers", Long.class)
				.mapKey("cnt", Long.class)
				.execute(Accumulators.getFirst());
		assertThat(mapRowCount).isEqualTo(0L);
	}
	
	@Test
	void crud_scalarMap_indexed() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
				.initializeWith(LinkedHashMap::new)
				.indexed();
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setName("john");
		List<String> expectedKeys = Arrays.asList("home", "mobile", "pro", "home2");
		Collections.shuffle(expectedKeys);
		Map<String, String> phoneNumbers = new LinkedHashMap<>();
		expectedKeys.forEach(expectedKey -> {
			// no matter if the value is the same for all keys since we don't check the Map content, but its keys order
			phoneNumbers.put(expectedKey, "01 23 45 67 89");
		});
		person.setPhoneNumbers(phoneNumbers);
		personPersister.insert(person);
		
		Person loaded = personPersister.select(person.getId());
		assertThat(loaded.getPhoneNumbers().keySet()).containsExactlyElementsOf(expectedKeys);
	}
	
	@Test
	void crud_scalarMap_fetchSeparately() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getPhoneNumbers, String.class, String.class).fetchSeparately();
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		Person person1 = new Person(new PersistableIdentifier<>(1L));
		person1.setName("john");
		Map<String, String> phoneNumbers1 = new LinkedHashMap<>();
		phoneNumbers1.put("home", "01 11 11 11 11");
		phoneNumbers1.put("mobile", "02 22 22 22 22");
		person1.setPhoneNumbers(phoneNumbers1);
		
		Person person2 = new Person(new PersistableIdentifier<>(2L));
		person2.setName("john");
		Map<String, String> phoneNumbers2 = new LinkedHashMap<>();
		phoneNumbers2.put("home", "03 33 33 33 33");
		phoneNumbers2.put("mobile", "04 44 44 44 44");
		person2.setPhoneNumbers(phoneNumbers2);
		
		personPersister.insert(person1, person2);
		
		Set<Person> loaded = personPersister.select(person1.getId(), person2.getId());
		Map<String, String> phoneNumbers = loaded.stream().flatMap(person -> person.getPhoneNumbers().entrySet().stream())
				.collect(Collectors.groupingBy(Map.Entry::getKey, LinkedHashMap::new, Collectors.mapping(Map.Entry::getValue, Collectors.joining(", "))));
		assertThat(phoneNumbers)
				.containsEntry("home", "01 11 11 11 11, 03 33 33 33 33")
				.containsEntry("mobile", "02 22 22 22 22, 04 44 44 44 44");
	}
	
	@Test
	void crud_scalarMap_fetchSeparately_indexed() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getPhoneNumbers, String.class, String.class)
				.fetchSeparately()
				.initializeWith(LinkedHashMap::new)
				.indexed();
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setName("john");
		List<String> expectedKeys = Arrays.asList("home", "mobile", "pro", "home2");
		Collections.shuffle(expectedKeys);
		Map<String, String> phoneNumbers = new LinkedHashMap<>();
		expectedKeys.forEach(expectedKey -> {
			// no matter if the value is the same for all keys since we don't check the Map content, but its keys order
			phoneNumbers.put(expectedKey, "01 23 45 67 89");
		});
		person.setPhoneNumbers(phoneNumbers);
		personPersister.insert(person);
		
		Person loaded = personPersister.select(person.getId());
		assertThat(loaded.getPhoneNumbers().keySet()).containsExactlyElementsOf(expectedKeys);
	}
	
	@Test
	void crudEnum() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getAddressBook, Person.AddressBookType.class, String.class);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setAddressBook(Maps.forHashMap(Person.AddressBookType.class, String.class)
				.add(Person.AddressBookType.HOME, "Grenoble")
				.add(Person.AddressBookType.BILLING_ADDRESS, "Lyon")
		);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getAddressBook()).isEqualTo(Maps.forHashMap(Person.AddressBookType.class, String.class)
				.add(Person.AddressBookType.HOME, "Grenoble")
				.add(Person.AddressBookType.BILLING_ADDRESS, "Lyon")
		);
		
		person.getAddressBook().remove(Person.AddressBookType.HOME);
		person.getAddressBook().put(Person.AddressBookType.OTHER, "Marseille");
		
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getAddressBook()).isEqualTo(Maps.forHashMap(Person.AddressBookType.class, String.class)
				.add(Person.AddressBookType.OTHER, "Marseille")
				.add(Person.AddressBookType.BILLING_ADDRESS, "Lyon")
		);
		
		personPersister.delete(loadedPerson);
		ExecutableQuery<String> stringExecutableQuery = persistenceContext.newQuery("select 'key' from Person_addressBook", String.class)
				.mapKey("key", String.class);
		Set<String> remainingAddressBook = stringExecutableQuery.execute(Accumulators.toSet());
		assertThat(remainingAddressBook).isEmpty();
	}
	
	@Test
	void crud_keyIsComplexType() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getAddresses, Timestamp.class, String.class)
				.withKeyMapping(embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate)
				);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
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
		ExecutableQuery<String> stringExecutableQuery = persistenceContext.newQuery("select 'key' from Person_addresses", String.class)
				.mapKey("key", String.class);
		Set<String> remainingAddressBook = stringExecutableQuery.execute(Accumulators.toSet());
		assertThat(remainingAddressBook).isEmpty();
	}
	
	@Test
	void crud_keyIsComplexType_fetchSeparately() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getAddresses, Timestamp.class, String.class)
				.withKeyMapping(embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate)
				).fetchSeparately();
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
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
	}
	
	@Test
	void crud_valueIsComplexType() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getContracts, String.class, Timestamp.class)
				.withValueMapping(FluentMappings.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate)
				);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
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
		ExecutableQuery<String> stringExecutableQuery = persistenceContext.newQuery("select 'key' from Person_contracts", String.class)
				.mapKey("key", String.class);
		Set<String> remainingAddressBook = stringExecutableQuery.execute(Accumulators.toSet());
		assertThat(remainingAddressBook).isEmpty();
	}
	
	@Test
	void crud_valueIsComplexType_fetchSeparately() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getContracts, String.class, Timestamp.class)
				.withValueMapping(FluentMappings.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate)
				).fetchSeparately();
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
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
	}
	
	@Test
	void crud_valueIsEntity_associationOnly() {
		FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = FluentMappings.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.map(Country::getDescription);
		EntityPersister<Country, Identifier<Long>> countryPersister = countryPersisterConfiguration.build(persistenceContext);
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getMapPropertyMadeOfEntityAsValue, String.class, Country.class)
				.withValueMapping(countryPersisterConfiguration)
				.cascading(CascadeOptions.RelationMode.ASSOCIATION_ONLY);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Country country1 = new Country(1);
		Country country2 = new Country(2);
		// because we're in ASSOCIATION_ONLY we must persist new values before the Map
		countryPersister.insert(Arrays.asList(country1, country2));
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setMapPropertyMadeOfEntityAsValue(Maps.forHashMap(String.class, Country.class)
				.add("Grenoble", country1)
				.add("Lyon", country2)
		);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		
		person.getMapPropertyMadeOfEntityAsValue().remove("Grenoble");
		// Changing entry value to check value is also updated
		Country country4 = new Country(4);
		Country country3 = new Country(3);
		// because we're in ASSOCIATION_ONLY we must persist new values before the Map
		countryPersister.insert(Arrays.asList(country3, country4));
		person.getMapPropertyMadeOfEntityAsValue().put("Lyon", country4);
		person.getMapPropertyMadeOfEntityAsValue().put("Marseille", country3);
		
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		ExecutableQuery<Long> longExecutableQuery2 = persistenceContext.newQuery("select valueId from Person_mapPropertyMadeOfEntityAsValue", Long.class)
				.mapKey("valueId", Long.class);
		Set<Long> remainingEntries = longExecutableQuery2.execute(Accumulators.toSet());
		assertThat(remainingEntries).containsExactlyInAnyOrder(4L, 3L);
		
		personPersister.delete(loadedPerson);
		ExecutableQuery<Long> longExecutableQuery1 = persistenceContext.newQuery("select valueId from Person_mapPropertyMadeOfEntityAsValue", Long.class)
				.mapKey("valueId", Long.class);
		remainingEntries = longExecutableQuery1.execute(Accumulators.toSet());
		assertThat(remainingEntries).isEmpty();
		
		// by default key entities are not deleted since cascading is not defined
		ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select id from Country", Long.class)
				.mapKey("id", Long.class);
		Set<Long> remainingCountries = longExecutableQuery.execute(Accumulators.toSet());
		assertThat(remainingCountries).containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
	}
	
	@Test
	void crud_keyAndValueIsComplexType() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = FluentMappings.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getMapPropertyMadeOfComplexTypes, Timestamp.class, Car.Radio.class)
				.withKeyMapping(FluentMappings.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate)
				)
				.withValueMapping(FluentMappings.embeddableBuilder(Car.Radio.class)
						.map(Car.Radio::getSerialNumber)
						.map(Car.Radio::getModel)
				);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		LocalDateTime now = LocalDateTime.now();
		Car.Radio radio1 = new Car.Radio("123");
		radio1.setModel("model1");
		Car.Radio radio2 = new Car.Radio("456");
		radio2.setModel("model2");
		person.setMapPropertyMadeOfComplexTypes(Maps.forHashMap(Timestamp.class, Car.Radio.class)
				.add(new Timestamp(now.minusDays(1), now.minusDays(1)), radio1)
				.add(new Timestamp(now.minusDays(2), now.minusDays(2)), radio2)
		);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getMapPropertyMadeOfComplexTypes()).isEqualTo(Maps.forHashMap(Timestamp.class, Car.Radio.class)
				.add(new Timestamp(now.minusDays(1), now.minusDays(1)), radio1)
				.add(new Timestamp(now.minusDays(2), now.minusDays(2)), radio2)
		);
		
		Car.Radio radio3 = new Car.Radio("789");
		radio3.setModel("model3");
		person.getMapPropertyMadeOfComplexTypes().remove(new Timestamp(now.minusDays(1), now.minusDays(1)));
		// Changing entry value to check value is also updated
		Car.Radio radio4 = new Car.Radio("789");
		radio4.setModel("model4");
		person.getMapPropertyMadeOfComplexTypes().put(new Timestamp(now.minusDays(2), now.minusDays(2)), radio4);
		person.getMapPropertyMadeOfComplexTypes().put(new Timestamp(now.minusDays(3), now.minusDays(3)), radio3);
		
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getMapPropertyMadeOfComplexTypes()).isEqualTo(Maps.forHashMap(Timestamp.class, Car.Radio.class)
				.add(new Timestamp(now.minusDays(2), now.minusDays(2)), radio4)
				.add(new Timestamp(now.minusDays(3), now.minusDays(3)), radio3)
		);
		
		personPersister.delete(loadedPerson);
		ExecutableQuery<String> stringExecutableQuery = persistenceContext.newQuery("select 'key' from Person_mapPropertyMadeOfComplexTypes", String.class)
				.mapKey("key", String.class);
		Set<String> remainingAddressBook = stringExecutableQuery.execute(Accumulators.toSet());
		assertThat(remainingAddressBook).isEmpty();
	}
	
	@Test
	void keyAndValueIsComplexType_schemaGeneration() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getMapPropertyMadeOfComplexTypesWithColumnDuplicates, Timestamp.class, Timestamp.class)
				.withKeyMapping(FluentMappings.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate))
				.overrideName(Timestamp::getCreationDate, "key_creation_date")
				.overrideName(Timestamp::getModificationDate, "key_modification_date")
				.withValueMapping(FluentMappings.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate).columnName("createdAt")
						.map(Timestamp::getModificationDate).columnName("modifiedAt"))
				.overrideName(Timestamp::getCreationDate, "value_creationDate")
				.overrideName(Timestamp::getModificationDate, "value_modificationDate");
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Map<String, Table<?>> tablePerName = map(DDLDeployer.collectTables(persistenceContext), Table::getName);
		assertThat(tablePerName.get("Person_mapPropertyMadeOfComplexTypesWithColumnDuplicates")
				.getColumns().stream().map(Column::getName)).containsExactlyInAnyOrder("id", "key_creation_date", "key_modification_date", "value_creationDate", "value_modificationDate");
	}
	
	@Test
	void crud_keyAndValueIsComplexType_overrideColumnName() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getMapPropertyMadeOfComplexTypesWithColumnDuplicates, Timestamp.class, Timestamp.class)
				.withKeyMapping(FluentMappings.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate)
				)
				.withValueMapping(FluentMappings.embeddableBuilder(Timestamp.class)
						.map(Timestamp::getCreationDate)
						.map(Timestamp::getModificationDate)
				)
				.overrideName(Timestamp::getCreationDate, "value_creationDate")
				.overrideName(Timestamp::getModificationDate, "value_modificationDate");
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
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
		ExecutableQuery<String> stringExecutableQuery = persistenceContext.newQuery("select 'key' from Person_mapPropertyMadeOfComplexTypesWithColumnDuplicates", String.class)
				.mapKey("key", String.class);
		Set<String> remainingAddressBook = stringExecutableQuery.execute(Accumulators.toSet());
		assertThat(remainingAddressBook).isEmpty();
	}
	
	@Test
	void crud_entityAsKeyMap() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getMapPropertyMadeOfEntityAsKey, Country.class, String.class)
				.withKeyMapping(entityBuilder(Country.class, LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription));
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(10L));
		person.setMapPropertyMadeOfEntityAsKey(new LinkedHashMap<>());
		person.getMapPropertyMadeOfEntityAsKey().put(new Country(1), "Grenoble");
		person.getMapPropertyMadeOfEntityAsKey().put(new Country(2), "Lyon");
		personPersister.insert(person);
		
		Person loaded = personPersister.select(person.getId());
		assertThat(loaded.getMapPropertyMadeOfEntityAsKey())
				.containsEntry(new Country(1), "Grenoble")
				.containsEntry(new Country(2), "Lyon");
		
		loaded.getMapPropertyMadeOfEntityAsKey().remove(new Country(1));
		loaded.getMapPropertyMadeOfEntityAsKey().put(new Country(2), "Paris");
		loaded.getMapPropertyMadeOfEntityAsKey().put(new Country(3), "Marseille");
		personPersister.update(loaded, person, true);
		
		Person reloaded = personPersister.select(person.getId());
		assertThat(reloaded.getMapPropertyMadeOfEntityAsKey())
				.containsOnlyKeys(new Country(2), new Country(3))
				.containsEntry(new Country(2), "Paris")
				.containsEntry(new Country(3), "Marseille");
		
		personPersister.delete(reloaded);
		
		Long mapRowCount = persistenceContext.newQuery("select count(*) as cnt from Person_mapPropertyMadeOfEntityAsKey", Long.class)
				.mapKey("cnt", Long.class)
				.execute(Accumulators.getFirst());
		assertThat(mapRowCount).isEqualTo(0L);
	}
	
	@Test
	void select_entityAsKeyMap_fetchSeparately() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getMapPropertyMadeOfEntityAsKey, Country.class, String.class)
				.withKeyMapping(entityBuilder(Country.class, LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription))
				.fetchSeparately();
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(10L));
		person.setMapPropertyMadeOfEntityAsKey(new LinkedHashMap<>());
		person.getMapPropertyMadeOfEntityAsKey().put(new Country(1), "Grenoble");
		person.getMapPropertyMadeOfEntityAsKey().put(new Country(2), "Lyon");
		personPersister.insert(person);
		
		Person loaded = personPersister.select(person.getId());
		assertThat(loaded.getMapPropertyMadeOfEntityAsKey())
				.containsEntry(new Country(1), "Grenoble")
				.containsEntry(new Country(2), "Lyon");
	}
	
	@Test
	void select_entityAsKeyMap_complexType_fetchSeparately() {
		Set<PersonId> persistedPersons = new HashSet<>();
		FluentEntityMappingBuilder<org.codefilarete.stalactite.engine.model.compositekey.Person, PersonId> personBuilder = entityBuilder(org.codefilarete.stalactite.engine.model.compositekey.Person.class, PersonId.class)
				.mapKey(org.codefilarete.stalactite.engine.model.compositekey.Person::getId, compositeKeyBuilder(PersonId.class)
						.map(PersonId::getFirstName)
						.map(PersonId::getLastName)
						.map(PersonId::getAddress), p -> persistedPersons.add(p.getId()), p -> persistedPersons.contains(p.getId()))
				.mapMap(org.codefilarete.stalactite.engine.model.compositekey.Person::getMapPropertyMadeOfCompositeIdEntityAsKey, House.class, String.class)
				.withKeyMapping(entityBuilder(House.class, HouseId.class)
						.mapKey(House::getHouseId, compositeKeyBuilder(HouseId.class)
								.map(HouseId::getNumber)
								.map(HouseId::getStreet)
								.map(HouseId::getZipCode)
								.map(HouseId::getCity)))
				.fetchSeparately();
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<org.codefilarete.stalactite.engine.model.compositekey.Person, PersonId> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		org.codefilarete.stalactite.engine.model.compositekey.Person person1 = new org.codefilarete.stalactite.engine.model.compositekey.Person(new PersonId("John", "Do", "nowhere"));
		person1.setMapPropertyMadeOfCompositeIdEntityAsKey(new LinkedHashMap<>());
		person1.getMapPropertyMadeOfCompositeIdEntityAsKey().put(new House(new HouseId(42, "Stalactite street", "142", "CodeFilarete City")), "home");
		person1.getMapPropertyMadeOfCompositeIdEntityAsKey().put(new House(new HouseId(43, "Another street", "143", "Another City")), "work");
		org.codefilarete.stalactite.engine.model.compositekey.Person person2 = new org.codefilarete.stalactite.engine.model.compositekey.Person(new PersonId("Jane", "Do", "nowhere"));
		person2.setMapPropertyMadeOfCompositeIdEntityAsKey(new LinkedHashMap<>());
		person2.getMapPropertyMadeOfCompositeIdEntityAsKey().put(new House(new HouseId(52, "Stalactite street", "242", "CodeFilarete City")), "home");
		person2.getMapPropertyMadeOfCompositeIdEntityAsKey().put(new House(new HouseId(53, "Another street", "243", "Another City")), "work");
		personPersister.insert(person1, person2);
		
		org.codefilarete.stalactite.engine.model.compositekey.Person loadedPerson1 = personPersister.select(person1.getId());
		// we extract the identifier from the map because it is comparable with equals() (by contract of Stalactite)
		// which allows AssertJ to check equality on it, else (with House instances) AssertJ won't be able to check
		// equality, except if with implement equals(..) on it, which we don't want to keep close to Stalactite philosophy
		Map<HouseId, String> homeTypePerAddress = map(loadedPerson1.getMapPropertyMadeOfCompositeIdEntityAsKey().entrySet(), e -> e.getKey().getHouseId(), Map.Entry::getValue);
		assertThat(homeTypePerAddress)
				.containsEntry(new HouseId(42, "Stalactite street", "142", "CodeFilarete City"), "home")
				.containsEntry(new HouseId(43, "Another street", "143", "Another City"), "work");
		
		
		Set<org.codefilarete.stalactite.engine.model.compositekey.Person> loadedPersons = personPersister.select(person1.getId(), person2.getId());
		// we extract the identifier from the map because it is comparable with equals() (by contract of Stalactite)
		// which allows AssertJ to check equality on it, else (with House instances) AssertJ won't be able to check
		// equality, except if with implement equals(..) on it, which we don't want to keep close to Stalactite philosophy
		Set<Map.Entry<House, String>> entries = loadedPersons.stream().flatMap(p -> p.getMapPropertyMadeOfCompositeIdEntityAsKey().entrySet().stream())
				.collect(Collectors.toSet());
		Map<HouseId, String> homeTypePerAddresses = map(entries, e -> e.getKey().getHouseId(), Map.Entry::getValue);
		assertThat(homeTypePerAddresses)
				.containsEntry(new HouseId(42, "Stalactite street", "142", "CodeFilarete City"), "home")
				.containsEntry(new HouseId(43, "Another street", "143", "Another City"), "work")
				.containsEntry(new HouseId(52, "Stalactite street", "242", "CodeFilarete City"), "home")
				.containsEntry(new HouseId(53, "Another street", "243", "Another City"), "work");
	}
	
	@Test
	void crud_entityAsValueMap() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getMapPropertyMadeOfEntityAsValue, String.class, Country.class)
				.withValueMapping(entityBuilder(Country.class, LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription));
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setMapPropertyMadeOfEntityAsValue(new LinkedHashMap<>());
		person.getMapPropertyMadeOfEntityAsValue().put("home", new Country(1));
		person.getMapPropertyMadeOfEntityAsValue().put("office", new Country(2));
		personPersister.insert(person);
		
		Person loaded = personPersister.select(person.getId());
		assertThat(loaded.getMapPropertyMadeOfEntityAsValue())
				.containsEntry("home", new Country(1))
				.containsEntry("office", new Country(2));
		
		loaded.getMapPropertyMadeOfEntityAsValue().remove("home");
		loaded.getMapPropertyMadeOfEntityAsValue().put("office", new Country(3));
		loaded.getMapPropertyMadeOfEntityAsValue().put("secondary", new Country(4));
		personPersister.update(loaded, person, true);
		
		Person reloaded = personPersister.select(person.getId());
		assertThat(reloaded.getMapPropertyMadeOfEntityAsValue())
				.containsOnlyKeys("office", "secondary")
				.containsEntry("office", new Country(3))
				.containsEntry("secondary", new Country(4));
		
		personPersister.delete(reloaded);
		
		Long mapRowCount = persistenceContext.newQuery("select count(*) as cnt from Person_mapPropertyMadeOfEntityAsValue", Long.class)
				.mapKey("cnt", Long.class)
				.execute(Accumulators.getFirst());
		assertThat(mapRowCount).isEqualTo(0L);
	}
	
	@Test
	void select_entityAsValueMap_fetchSeparately() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getMapPropertyMadeOfEntityAsValue, String.class, Country.class)
				.withValueMapping(entityBuilder(Country.class, LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription))
				.fetchSeparately();
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		new DDLDeployer(persistenceContext).deployDDL();
		
		Person person1 = new Person(new PersistableIdentifier<>(1L));
		person1.setMapPropertyMadeOfEntityAsValue(new LinkedHashMap<>());
		person1.getMapPropertyMadeOfEntityAsValue().put("home", new Country(1));
		person1.getMapPropertyMadeOfEntityAsValue().put("office", new Country(2));
		Person person2 = new Person(new PersistableIdentifier<>(2L));
		person2.setMapPropertyMadeOfEntityAsValue(new LinkedHashMap<>());
		person2.getMapPropertyMadeOfEntityAsValue().put("home", new Country(3));
		person2.getMapPropertyMadeOfEntityAsValue().put("office", new Country(4));
		personPersister.insert(Arrays.asList(person1, person2));
		
		Set<Person> loaded = personPersister.select(person1.getId(), person2.getId());
		Map<Identifier<Long>, Person> personMap = map(loaded, Person::getId);
		assertThat(personMap.get(person1.getId()).getMapPropertyMadeOfEntityAsValue())
				.containsEntry("home", new Country(1))
				.containsEntry("office", new Country(2));
		assertThat(personMap.get(person2.getId()).getMapPropertyMadeOfEntityAsValue())
				.containsEntry("home", new Country(3))
				.containsEntry("office", new Country(4));
	}
	
	@Test
	void crud_keyAndValueAreEntities() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getMapPropertyMadeOfEntityAsKeyAndValue, City.class, Country.class)
				.withKeyMapping(FluentMappings.entityBuilder(City.class, Identifier.LONG_TYPE)
						.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(City::getName)
				)
				.withValueMapping(FluentMappings.entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Country::getName)
				);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setMapPropertyMadeOfEntityAsKeyAndValue(Maps.forHashMap(City.class, Country.class)
				.add(new City(1, "Grenoble"), new Country(1))
				.add(new City(2, "Lyon"), new Country(2))
		);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getMapPropertyMadeOfEntityAsKeyAndValue()).isEqualTo(Maps.forHashMap(City.class, Country.class)
				.add(new City(1, "Grenoble"), new Country(1))
				.add(new City(2, "Lyon"), new Country(2))
		);
		
		person.getMapPropertyMadeOfEntityAsKeyAndValue().remove(new City(1));
		// Changing entry value to check value is also updated
		person.getMapPropertyMadeOfEntityAsKeyAndValue().put(new City(2, "Paris"), new Country(2));
		person.getMapPropertyMadeOfEntityAsKeyAndValue().put(new City(3, "Marseille"), new Country(3));
		
		personPersister.update(person, loadedPerson, true);
		
		loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getMapPropertyMadeOfEntityAsKeyAndValue())
				.isEqualTo(Maps.forHashMap(City.class, Country.class)
						.add(new City(2, "Paris"), new Country(2))
						.add(new City(3, "Marseille"), new Country(3))
				);
		
		personPersister.delete(loadedPerson);
		ExecutableQuery<String> stringExecutableQuery = persistenceContext.newQuery("select 'key' from Person_mapPropertyMadeOfEntityAsKeyAndValue", String.class)
				.mapKey("key", String.class);
		Set<String> remainingEntries = stringExecutableQuery.execute(Accumulators.toSet());
		assertThat(remainingEntries).isEmpty();
		
		// by default key entities are not deleted since cascading is not defined
		ExecutableQuery<Long> longExecutableQuery1 = persistenceContext.newQuery("select id from City", Long.class)
				.mapKey("id", Long.class);
		Set<Long> remainingCities = longExecutableQuery1.execute(Accumulators.toSet());
		assertThat(remainingCities).containsExactlyInAnyOrder(1L, 2L, 3L);
		
		ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select id from Country", Long.class)
				.mapKey("id", Long.class);
		Set<Long> remainingCountries = longExecutableQuery.execute(Accumulators.toSet());
		assertThat(remainingCountries).containsExactlyInAnyOrder(1L, 2L, 3L);
	}
	
	@Test
	void select_keyAndValueAreEntities_bothFetchSeparately() {
		FluentEntityMappingBuilder<Person, Identifier<Long>> personBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapMap(Person::getMapPropertyMadeOfEntityAsKeyAndValue, City.class, Country.class)
				.withKeyMapping(FluentMappings.entityBuilder(City.class, Identifier.LONG_TYPE)
						.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(City::getName)
				)
				.fetchSeparately()
				.withValueMapping(FluentMappings.entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(Country::getName)
				)
				.fetchSeparately();
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Person, Identifier<Long>> personPersister = testInstance.resolve(personBuilder.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setMapPropertyMadeOfEntityAsKeyAndValue(Maps.forHashMap(City.class, Country.class)
				.add(new City(1, "Grenoble"), new Country(1))
				.add(new City(2, "Lyon"), new Country(2))
		);
		
		personPersister.insert(person);
		
		Person loadedPerson = personPersister.select(person.getId());
		assertThat(loadedPerson.getMapPropertyMadeOfEntityAsKeyAndValue()).isEqualTo(Maps.forHashMap(City.class, Country.class)
				.add(new City(1, "Grenoble"), new Country(1))
				.add(new City(2, "Lyon"), new Country(2))
		);
	}
}

