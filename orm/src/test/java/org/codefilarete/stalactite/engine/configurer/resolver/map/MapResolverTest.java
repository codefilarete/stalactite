package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;

import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;

class MapResolverTest {

	private final Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private PersistenceContext persistenceContext;

	@BeforeEach
	void setUp() {
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		persistenceContext = new PersistenceContext(dataSource, dialect);
	}

	@Test
	void crud_scalarMap_insertUpdateDelete() {
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
	void crud_entityAsKeyMap_insertUpdateDelete() {
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

		Person person = new Person(new PersistableIdentifier<>(1L));
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
	void crud_entityAsValueMap_insertUpdateDelete() {
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
}

