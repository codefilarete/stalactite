package org.codefilarete.stalactite.spring.repository.config;

import javax.sql.DataSource;
import java.util.Optional;

import org.codefilarete.stalactite.engine.ColumnOptions;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.spring.repository.SimpleStalactiteRepository;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
@SpringJUnitConfig(classes = {
		StalactiteRepositoryConfigExtensionTest.StalactiteRepositoryContextConfiguration.class
})
@EnableStalactiteRepositories(basePackages = "org.codefilarete.stalactite.spring.repository.config")
class StalactiteRepositoryConfigExtensionTest {
    
	@Autowired
	private DummyStalactiteRepository dummyStalactiteRepository;
	
	@Autowired
	private PersistenceContext persistenceContext;
	
	@Test
	void injectionIsDone() {
		// Line below serves no purpose actually since Spring will throw an error if field is not injected
		// but a test without assertion is not a test ;)
		assertThat(dummyStalactiteRepository).isNotNull();
		
		Table person = new Table<>("Person");
		Column<Table<?>, Long> idColumn = person.addColumn("id", Long.class);
		Column<Table<?>, String> nameColumn = person.addColumn("name", String.class);
		persistenceContext.insert(person)
				.set(idColumn, 1L)
				.set(nameColumn, "John Do")
				.execute();
		Optional<Person> loadedPerson = dummyStalactiteRepository.findById(new PersistedIdentifier<>(1L));
		assertThat(loadedPerson).isNotEmpty();
	}
	
	@Test
	void crud() {
		Person person = new Person(42);
		person.setName("Toto");
		
		// trying to insert
		dummyStalactiteRepository.save(person);
		Optional<Person> loadedPerson = dummyStalactiteRepository.findById(new PersistedIdentifier<>(42L));
		assertThat(loadedPerson).isNotEmpty();
		
		// trying with update
		person.setName("Titi");
		dummyStalactiteRepository.save(person);
		loadedPerson = dummyStalactiteRepository.findById(new PersistedIdentifier<>(42L));
		assertThat(loadedPerson).map(Person::getName).get().isEqualTo("Titi");
		
		dummyStalactiteRepository.delete(person);
		loadedPerson = dummyStalactiteRepository.findById(new PersistedIdentifier<>(42L));
		assertThat(loadedPerson).isEmpty();
	}
	
	public static class StalactiteRepositoryContextConfiguration {
		
		@Bean
		public DataSource dataSource() {
			return new HSQLDBInMemoryDataSource();
		}
		
		/**
		 * A {@link PlatformTransactionManager} is required by {@link SimpleStalactiteRepository}
		 * because it is annotated with @{@link org.springframework.transaction.annotation.Transactional}.
		 * By default, Spring expects it to be named {@literal transactionManager}
		 * 
		 * @param dataSource the {@link DataSource} required by returned {@link PlatformTransactionManager}
		 * @return a simple {@link DataSourceTransactionManager} for the tests
		 */
		@Bean
		public PlatformTransactionManager transactionManager(DataSource dataSource) {
			return new DataSourceTransactionManager(dataSource);
		}
		
		@Bean
		public PersistenceContext persistenceContext(DataSource dataSource) {
			Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
			dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
			dialect.getSqlTypeRegistry().put(Identifier.class, "int");
			
			return new PersistenceContext(dataSource, dialect);
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