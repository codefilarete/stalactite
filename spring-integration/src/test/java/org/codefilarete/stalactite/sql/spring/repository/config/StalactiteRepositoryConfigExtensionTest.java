package org.codefilarete.stalactite.sql.spring.repository.config;

import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.codefilarete.stalactite.engine.ColumnOptions;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

/**
 * @author Guillaume Mary
 */
@SpringBootTest(classes = {
		StalactiteRepositoryConfigExtensionTest.StalactiteRepositoryContextConfiguration.class
})
//@EnableStalactiteRepositories(basePackages = "org.codefilarete.stalactite.sql.spring.repository.config")
//@ContextConfiguration(classes = StalactiteRepositoryConfigExtensionTest.StalactiteRepositoryContextConfiguration.class)
class StalactiteRepositoryConfigExtensionTest {
    
	@Autowired
	private DummyStalactiteRepository dummyStalactiteRepository;
	
	@Autowired
	private PersistenceContext persistenceContext;
	
	@Test
	void injectionIsDone() {
		// Line below serves no purpose actually since Spring will throw an error if field is not injected
		// but a test without assertion is not a test ;)
		Assertions.assertThat(dummyStalactiteRepository).isNotNull();
		
		Table person = new Table<>("Person");
		Column<Table<?>, Long> idColumn = person.addColumn("id", Long.class);
		Column<Table<?>, String> nameColumn = person.addColumn("name", String.class);
		persistenceContext.<Table<?>>insert(person)
				.set(idColumn, 1L)
				.set(nameColumn, "John Do")
				.execute();
		Optional<Person> loadedPerson = dummyStalactiteRepository.findById(new PersistedIdentifier<>(1L));
		Assertions.assertThat(loadedPerson).isNotEmpty();
	}
	
	@EnableStalactiteRepositories(basePackages = "org.codefilarete.stalactite.sql.spring.repository.config")
	public static class StalactiteRepositoryContextConfiguration {
		
		@Bean
		public PersistenceContext persistenceContext() {
			HSQLDBDialect dialect = new HSQLDBDialect();
			HSQLDBInMemoryDataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
			dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
			dialect.getSqlTypeRegistry().put(Identifier.class, "int");
			
			return new PersistenceContext(inMemoryDataSource, dialect);
		}
		
		@Bean
		public EntityPersister<Person, Identifier<Long>> personPersister(PersistenceContext persistenceContext) {
			return MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
					.mapKey(Person::getId, ColumnOptions.IdentifierPolicy.alreadyAssigned(p -> ((Person) p).getId().setPersisted(), p -> ((Person) p).getId().isPersisted()))
					.map(Person::getName)
					.build(persistenceContext);
		}
		
		@EventListener
		public void onApplicationEvent(ContextRefreshedEvent event) {
			DDLDeployer ddlDeployer = new DDLDeployer(event.getApplicationContext().getBean(PersistenceContext.class));
			ddlDeployer.deployDDL();
		}
	}
}