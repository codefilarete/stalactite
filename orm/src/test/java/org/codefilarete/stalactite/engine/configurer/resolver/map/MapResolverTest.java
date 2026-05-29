package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;

import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver;
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
}

