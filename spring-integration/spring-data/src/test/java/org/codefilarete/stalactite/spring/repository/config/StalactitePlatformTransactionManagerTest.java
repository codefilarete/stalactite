package org.codefilarete.stalactite.spring.repository.config;

import javax.sql.DataSource;
import java.util.Optional;

import org.codefilarete.stalactite.engine.ColumnOptions;
import org.codefilarete.stalactite.engine.CurrentThreadTransactionalConnectionProvider;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.spring.repository.config.StalactitePlatformTransactionManagerTest.StalactiteTransactionalContextConfiguration;
import org.codefilarete.stalactite.spring.transaction.StalactitePlatformTransactionManager;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * @author Guillaume Mary
 */
@SpringJUnitConfig(classes = {
		StalactiteTransactionalContextConfiguration.class
})
@EnableStalactiteRepositories(basePackages = "org.codefilarete.stalactite.spring.repository.config")
class StalactitePlatformTransactionManagerTest {
    
	@Autowired
	private DummyStalactiteRepository dummyStalactiteRepository;
	
	@Nested
	@TestMethodOrder(OrderAnnotation.class)
	class WithRepository {
		
		// No @Transactional hence no rollback, the transaction only relies on SimpleStalactiteRepository.save(..)
		// which impact the "createSameDataAgain" that can't persist the same data due to id constraint 
		@Test
		@Order(1)
		void createData() {
			Person person = new Person(42);
			person.setName("Toto");
			dummyStalactiteRepository.save(person);
			// we just check that data exist, else test serves no purpose
			Optional<Person> loadedPerson = dummyStalactiteRepository.findById(new PersistedIdentifier<>(42L));
			assertThat(loadedPerson).isNotEmpty();
		}
		
		@Test
		@Order(2)
		void createSameDataAgain() {
			Person person = new Person(42);
			person.setName("Tata");
			
			// trying to insert : this will throw an exception if data already exists due to primary key conflict
			assertThatCode(() -> dummyStalactiteRepository.save(person))
					.hasRootCauseMessage("integrity constraint violation: unique constraint or index violation; SYS_CT_10093 table: PERSON");
		}
	}
	
	public static class StalactiteTransactionalContextConfiguration {
		
		@Bean
		public DataSource dataSource() {
			return new HSQLDBInMemoryDataSource();
		}
		
		@Bean
		public StalactitePlatformTransactionManager transactionManager(DataSource dataSource) {
			return new StalactitePlatformTransactionManager(dataSource);
		}
		
		@Bean
		public PersistenceContext persistenceContext(StalactitePlatformTransactionManager transactionManager) {
			HSQLDBDialect dialect = new HSQLDBDialect();
			dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
			dialect.getSqlTypeRegistry().put(Identifier.class, "int");
			
			return new PersistenceContext(transactionManager, dialect);
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
			PersistenceContext persistenceContext = event.getApplicationContext().getBean(PersistenceContext.class);
			DataSource dataSource = event.getApplicationContext().getBean(DataSource.class);
			Dialect dialect = persistenceContext.getDialect();
			// Note that we use a CurrentThreadTransactionalConnectionProvider instead of existing StalactitePlatformTransactionManager
			// because Transaction doesn't exist yet, even by marking @Transactional current method 
			DDLDeployer ddlDeployer = new DDLDeployer(dialect.getDdlTableGenerator(), dialect.getDdlSequenceGenerator(), new CurrentThreadTransactionalConnectionProvider(dataSource));
			ddlDeployer.getDdlGenerator().addTables(DDLDeployer.collectTables(persistenceContext));
			ddlDeployer.deployDDL();
		}
	}
}