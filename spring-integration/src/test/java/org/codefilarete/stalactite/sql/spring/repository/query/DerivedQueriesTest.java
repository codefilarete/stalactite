package org.codefilarete.stalactite.sql.spring.repository.query;


import javax.sql.DataSource;
import java.util.Arrays;
import java.util.Set;

import org.codefilarete.stalactite.engine.ColumnOptions;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.TransactionalConnectionProvider;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.spring.repository.config.EnableStalactiteRepositories;
import org.codefilarete.stalactite.sql.spring.transaction.StalactitePlatformTransactionManager;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * @author Guillaume Mary
 */
@SpringJUnitConfig(classes = {
		DerivedQueriesTest.StalactiteRepositoryContextConfiguration.class
})
@Transactional
@EnableStalactiteRepositories(basePackages = "org.codefilarete.stalactite.sql.spring.repository.query")
class DerivedQueriesTest {
	
	@Autowired
	private DerivedQueriesRepository derivedQueriesRepository;
	
	@Nested
	class EqualCriteria {
		
		@Test
		void oneCriteria_oneResult() {
			Country country1 = new Country(42);
			country1.setName("Toto");
			derivedQueriesRepository.save(country1);
			
			Country loadedCountry = derivedQueriesRepository.findByName("Toto");
			assertThat(loadedCountry).isEqualTo(country1);
		}
		
		@Test
		void twoCriteria_severalResults_throwsException() {
			Country country1 = new Country(42);
			country1.setName("Toto");
			Country country2 = new Country(43);
			country2.setName("Toto");
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));

			Country loadedCountry = derivedQueriesRepository.findByIdAndName(new PersistedIdentifier<>(42L), "Toto");
			assertThat(loadedCountry).isEqualTo(country1);
		}
		
		@Test
		void oneCriteria_severalResults_throwsException() {
			Country country1 = new Country(42);
			country1.setName("Toto");
			Country country2 = new Country(43);
			country2.setName("Toto");
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			assertThatCode(() -> derivedQueriesRepository.findByName("Toto"))
					.isInstanceOf(Accumulators.NonUniqueObjectException.class);
		}
	}
	
	@Nested
	class LikeCriteria {
		
		@Test
		void oneCriteria_severalResults() {
			Country country1 = new Country(42);
			country1.setDescription("a description with a keyword");
			Country country2 = new Country(43);
			country2.setDescription("a keyword contains in the description");
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));
			
			Set<Country> loadedCountries = derivedQueriesRepository.findByDescriptionLike("keyword");
			assertThat(loadedCountries).containsExactlyInAnyOrder(country1, country2);
		}
	
	}
	
	public static class StalactiteRepositoryContextConfiguration {
		
		@Bean
		public DataSource dataSource() {
			return new HSQLDBInMemoryDataSource();
		}
		
		@Bean
		public StalactitePlatformTransactionManager transactionManager(DataSource dataSource) {
			return new StalactitePlatformTransactionManager(dataSource);
		}
		
		@Bean
		public PersistenceContext persistenceContext(StalactitePlatformTransactionManager dataSource) {
			HSQLDBDialect dialect = new HSQLDBDialect();
			dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
			dialect.getSqlTypeRegistry().put(Identifier.class, "int");
			
			return new PersistenceContext(dataSource, dialect);
		}
		
		@Bean
		public EntityPersister<Country, Identifier<Long>> countryPersister(PersistenceContext persistenceContext) {
			return MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ColumnOptions.IdentifierPolicy.<Country, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
					.map(Country::getName)
					.map(Country::getDescription)
					.build(persistenceContext);
		}
		
		@EventListener
		public void onApplicationEvent(ContextRefreshedEvent event) {
			PersistenceContext persistenceContext = event.getApplicationContext().getBean(PersistenceContext.class);
			DataSource dataSource = event.getApplicationContext().getBean(DataSource.class);
			Dialect dialect = persistenceContext.getDialect();
			DDLDeployer ddlDeployer = new DDLDeployer(dialect.getDdlTableGenerator(), new TransactionalConnectionProvider(dataSource));
			ddlDeployer.getDdlGenerator().addTables(DDLDeployer.collectTables(persistenceContext));
			ddlDeployer.deployDDL();
		}
	}
	
}