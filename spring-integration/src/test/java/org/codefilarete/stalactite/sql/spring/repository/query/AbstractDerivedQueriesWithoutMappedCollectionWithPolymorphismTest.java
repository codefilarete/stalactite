package org.codefilarete.stalactite.sql.spring.repository.query;

import javax.sql.DataSource;
import java.util.Set;

import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.TransactionalConnectionProvider;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.spring.repository.config.EnableStalactiteRepositories;
import org.codefilarete.stalactite.sql.spring.transaction.StalactitePlatformTransactionManager;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
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
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.springframework.data.domain.Sort.by;

/**
 * Dedicated test class for orderBy and limit cases : both work only with a mapping that doesn't imply Collection property
 * 
 * @author Guillaume Mary
 */
@Transactional
@EnableStalactiteRepositories(basePackages = "org.codefilarete.stalactite.sql.spring.repository.query",
		// because we have another repository in the same package, we filter them to keep only the appropriate one (it also checks that filtering works !)
		includeFilters = @Filter(
				type = FilterType.ASSIGNABLE_TYPE,
				classes = DerivedQueriesWithoutMappedCollectionRepository.class)
)
abstract class AbstractDerivedQueriesWithoutMappedCollectionWithPolymorphismTest {
	
	@Autowired
	private DerivedQueriesWithoutMappedCollectionRepository derivedQueriesRepository;
	
	@Test
	void limit() {
		Country country1 = new Republic(42);
		country1.setName("Toto");
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		
		Country country2 = new Republic(43);
		country2.setName("Tata");
		Person president2 = new Person(237);
		president2.setName("you");
		country2.setPresident(president2);
		
		Country country3 = new Republic(44);
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
	
	@Test
	void pageable() {
		Country country1 = new Republic(42);
		country1.setName("Titi");
		Country country2 = new Republic(43);
		country2.setName("Toto");
		Country country3 = new Republic(44);
		country3.setName("Tata");
		Country country4 = new Republic(45);
		country4.setName("Tutu");
		Country country5 = new Republic(46);
		country5.setName("Tonton");
		Country country6 = new Republic(47);
		country6.setName("TinTin");
		Country country7 = new Republic(48);
		country7.setName("Toutou");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4, country5, country6, country7));
		
		Page<Country> loadedCountries;
		
		loadedCountries = derivedQueriesRepository.findByNameLikeOrderByIdAsc("T", PageRequest.ofSize(2));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(4);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(7);
		assertThat(loadedCountries.get()).containsExactly(country1, country2);
		
		loadedCountries = derivedQueriesRepository.findByNameLikeOrderByIdAsc("T%o", PageRequest.ofSize(2));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(2);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(3);
		assertThat(loadedCountries.get()).containsExactly(country2, country5);
		
		loadedCountries = derivedQueriesRepository.findByNameLikeOrderByIdAsc("T", PageRequest.of(1, 2));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(4);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(7);
		assertThat(loadedCountries.get()).containsExactly(country3, country4);
		
		loadedCountries = derivedQueriesRepository.findByNameLikeOrderByIdAsc("T%o", PageRequest.of(1, 2));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(2);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(3);
		assertThat(loadedCountries.get()).containsExactly(country7);
	}
	
	@Test
	void pageable_withOrder() {
		Country country1 = new Republic(42);
		country1.setName("Titi");
		Country country2 = new Republic(43);
		country2.setName("Toto");
		Country country3 = new Republic(44);
		country3.setName("Tata");
		Country country4 = new Republic(45);
		country4.setName("Tutu");
		Country country5 = new Republic(46);
		country5.setName("Tonton");
		Country country6 = new Republic(47);
		country6.setName("TinTin");
		Country country7 = new Republic(48);
		country7.setName("Toutou");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4, country5, country6, country7));
		
		Page<Country> loadedCountries;
		
		loadedCountries = derivedQueriesRepository.findByNameLikeOrderByIdAsc("T", PageRequest.ofSize(2).withSort(by("id").ascending()));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(4);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(7);
		assertThat(loadedCountries.get()).containsExactly(country1, country2);
		
		loadedCountries = derivedQueriesRepository.findByNameLikeOrderByIdAsc("T%o", PageRequest.ofSize(2).withSort(by("id").ascending()));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(2);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(3);
		assertThat(loadedCountries.get()).containsExactly(country2, country5);
		
		loadedCountries = derivedQueriesRepository.findByNameLikeOrderByIdAsc("T", PageRequest.of(1, 2).withSort(by("id").ascending()));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(4);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(7);
		assertThat(loadedCountries.get()).containsExactly(country3, country4);
		
		loadedCountries = derivedQueriesRepository.findByNameLikeOrderByIdAsc("T%o", PageRequest.of(1, 2).withSort(by("id").ascending()));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(2);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(3);
		assertThat(loadedCountries.get()).containsExactly(country7);
	}
	
	@Test
	void slice() {
		Country country1 = new Republic(42);
		country1.setName("Titi");
		Country country2 = new Republic(43);
		country2.setName("Toto");
		Country country3 = new Republic(44);
		country3.setName("Tata");
		Country country4 = new Republic(45);
		country4.setName("Tutu");
		Country country5 = new Republic(46);
		country5.setName("Tonton");
		Country country6 = new Republic(47);
		country6.setName("TinTin");
		Country country7 = new Republic(48);
		country7.setName("Toutou");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4, country5, country6, country7));
		
		Slice<Country> loadedCountries = derivedQueriesRepository.searchByNameLikeOrderByIdAsc("T", PageRequest.ofSize(2));
		assertThat(loadedCountries.get()).containsExactly(country1, country2);
		assertThat(loadedCountries.getContent()).containsExactly(country1, country2);
		assertThat(loadedCountries.hasNext()).isTrue();
		assertThat(loadedCountries.nextPageable()).isEqualTo(PageRequest.of(1, 2));
		
		loadedCountries = derivedQueriesRepository.searchByNameLikeOrderByIdAsc("T%o", PageRequest.of(1, 2));
		assertThat(loadedCountries.get()).containsExactly(country7);
		assertThat(loadedCountries.nextPageable()).isEqualTo(Pageable.unpaged());
	}
	
	@Test
	void slice_withOrder() {
		Country country1 = new Republic(42);
		country1.setName("Titi");
		Country country2 = new Republic(43);
		country2.setName("Toto");
		Country country3 = new Republic(44);
		country3.setName("Tata");
		Country country4 = new Republic(45);
		country4.setName("Tutu");
		Country country5 = new Republic(46);
		country5.setName("Tonton");
		Country country6 = new Republic(47);
		country6.setName("TinTin");
		Country country7 = new Republic(48);
		country7.setName("Toutou");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4, country5, country6, country7));
		
		Slice<Country> loadedCountries = derivedQueriesRepository.searchByNameLikeOrderByIdAsc("T", PageRequest.ofSize(2).withSort(by("id").ascending()));
		assertThat(loadedCountries.get()).containsExactly(country1, country2);
		assertThat(loadedCountries.getContent()).containsExactly(country1, country2);
		assertThat(loadedCountries.hasNext()).isTrue();
		assertThat(loadedCountries.nextPageable()).isEqualTo(PageRequest.of(1, 2).withSort(by("id").ascending()));
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
