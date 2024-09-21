package org.codefilarete.stalactite.sql.spring.repository.query;

import javax.sql.DataSource;
import java.util.Set;

import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.TransactionalConnectionProvider;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.spring.repository.config.EnableStalactiteRepositories;
import org.codefilarete.stalactite.sql.spring.repository.query.DerivedQueriesWithoutMappedCollectionTest.StalactiteRepositoryContextConfigurationWithoutCollection;
import org.codefilarete.stalactite.sql.spring.transaction.StalactitePlatformTransactionManager;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;

/**
 * Dedicated test class for orderBy and limit cases : both work only with a mapping that doesn't imply Collection property
 * 
 * @author Guillaume Mary
 */
@SpringJUnitConfig(classes = {
		StalactiteRepositoryContextConfigurationWithoutCollection.class
})
@Transactional
@EnableStalactiteRepositories(basePackages = "org.codefilarete.stalactite.sql.spring.repository.query",
		// because we have another repository in the same package, we filter them to keep only the appropriate one (it also checks that filtering works !)
		includeFilters = @Filter(
				type = FilterType.ASSIGNABLE_TYPE,
				classes = DerivedQueriesWithoutMappedCollectionRepository.class)
)
public class DerivedQueriesWithoutMappedCollectionTest {
	
	@Autowired
	private DerivedQueriesWithoutMappedCollectionRepository derivedQueriesRepository;
	
	@Test
	void limit() {
		Country country1 = new Country(42);
		country1.setName("Toto");
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		
		Country country2 = new Country(43);
		country2.setName("Tata");
		Person president2 = new Person(237);
		president2.setName("you");
		country2.setPresident(president2);
		
		Country country3 = new Country(44);
		country3.setName("Titi");
		
		Vehicle vehicle = new Vehicle(1438L);
		vehicle.setColor(new Color(123));
		president1.setVehicle(vehicle);
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3));

		Country loadedCountry = derivedQueriesRepository.findFirstByOrderByNameAsc();
		assertThat(loadedCountry).isEqualTo(country2);
		
		Set<Country> loadedCountries = derivedQueriesRepository.findTop2ByOrderByNameAsc();
		assertThat(loadedCountries).containsExactly(country2, country3);
	}
	
	@Nested
	class Paging {
		
		@Test
		void pageable() {
			Country country1 = new Country(42);
			country1.setName("Titi");
			Country country2 = new Country(43);
			country2.setName("Toto");
			Country country3 = new Country(44);
			country3.setName("Tata");
			Country country4 = new Country(45);
			country4.setName("Tutu");
			Country country5 = new Country(46);
			country5.setName("Tonton");
			Country country6 = new Country(47);
			country6.setName("TinTin");
			Country country7 = new Country(48);
			country7.setName("Toutou");
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4, country5, country6, country7));
			
			Page<Country> loadedCountries;
			
			loadedCountries = derivedQueriesRepository.findByNameLike("T", PageRequest.ofSize(2));
			assertThat(loadedCountries.getTotalPages()).isEqualTo(4);
			assertThat(loadedCountries.getTotalElements()).isEqualTo(7);
			assertThat(loadedCountries.get()).containsExactly(country1, country2);
			
			loadedCountries = derivedQueriesRepository.findByNameLike("T%o", PageRequest.ofSize(2));
			assertThat(loadedCountries.getTotalPages()).isEqualTo(2);
			assertThat(loadedCountries.getTotalElements()).isEqualTo(3);
			assertThat(loadedCountries.get()).containsExactly(country2, country5);
			
			loadedCountries = derivedQueriesRepository.findByNameLike("T", PageRequest.of(1, 2));
			assertThat(loadedCountries.getTotalPages()).isEqualTo(4);
			assertThat(loadedCountries.getTotalElements()).isEqualTo(7);
			assertThat(loadedCountries.get()).containsExactly(country3, country4);
			
			loadedCountries = derivedQueriesRepository.findByNameLike("T%o", PageRequest.of(1, 2));
			assertThat(loadedCountries.getTotalPages()).isEqualTo(2);
			assertThat(loadedCountries.getTotalElements()).isEqualTo(3);
			assertThat(loadedCountries.get()).containsExactly(country7);
		}
		
		@Test
		void slice() {
			Country country1 = new Country(42);
			country1.setName("Titi");
			Country country2 = new Country(43);
			country2.setName("Toto");
			Country country3 = new Country(44);
			country3.setName("Tata");
			Country country4 = new Country(45);
			country4.setName("Tutu");
			Country country5 = new Country(46);
			country5.setName("Tonton");
			Country country6 = new Country(47);
			country6.setName("TinTin");
			Country country7 = new Country(48);
			country7.setName("Toutou");
			derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4, country5, country6, country7));
			
			Slice<Country> loadedCountries = derivedQueriesRepository.searchByNameLike("T", PageRequest.ofSize(2));
			assertThat(loadedCountries.get()).containsExactly(country1, country2);
			assertThat(loadedCountries.getContent()).containsExactly(country1, country2);
			assertThat(loadedCountries.hasNext()).isTrue();
			assertThat(loadedCountries.nextPageable()).isEqualTo(PageRequest.of(1, 2));
			
			loadedCountries = derivedQueriesRepository.searchByNameLike("T%o", PageRequest.of(1, 2));
			assertThat(loadedCountries.get()).containsExactly(country7);
			assertThat(loadedCountries.nextPageable()).isEqualTo(Pageable.unpaged());
		}
	}
	
	public static class StalactiteRepositoryContextConfigurationWithoutCollection {
		
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
			dialect.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
			dialect.getSqlTypeRegistry().put(Identifier.class, "int");
			
			dialect.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
			dialect.getSqlTypeRegistry().put(Color.class, "int");
			
			return new PersistenceContext(dataSource, dialect);
		}
		
		@Bean
		public EntityPersister<Country, Identifier<Long>> countryPersister(PersistenceContext persistenceContext) {
			return entityBuilder(Country.class, LONG_TYPE)
					.mapKey(Country::getId, IdentifierPolicy.<Country, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
					.map(Country::getName)
					.map(Country::getDescription)
					.map(Country::isEuMember)
					.embed(Country::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.mapOneToOne(Country::getPresident, entityBuilder(Person.class, LONG_TYPE)
							.mapKey(Person::getId, IdentifierPolicy.<Person, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
							.map(Person::getName)
							.mapCollection(Person::getNicknames, String.class)
							.mapOneToOne(Person::getVehicle, entityBuilder(Vehicle.class, LONG_TYPE)
									.mapKey(Vehicle::getId, IdentifierPolicy.<Vehicle, Identifier<Long>>alreadyAssigned(p -> p.getId().setPersisted(), p -> p.getId().isPersisted()))
									.map(Vehicle::getColor)))
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
