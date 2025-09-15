package org.codefilarete.stalactite.spring.repository.query;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.stalactite.engine.CurrentThreadTransactionalConnectionProvider;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.model.AbstractCountry;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.config.EnableStalactiteRepositories;
import org.codefilarete.stalactite.spring.repository.query.DerivedQueriesWithoutMappedCollectionRepository.NamesOnly;
import org.codefilarete.stalactite.spring.transaction.StalactitePlatformTransactionManager;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
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
import static org.codefilarete.stalactite.spring.repository.query.DerivedQueriesWithoutMappedCollectionRepository.NamesOnlyWithValue;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.tool.function.Functions.chain;
import static org.springframework.data.domain.Sort.by;

/**
 * Dedicated test class for orderBy and limit cases : both work only with a mapping that doesn't imply Collection property
 * 
 * @author Guillaume Mary
 */
@Transactional
@EnableStalactiteRepositories(basePackages = "org.codefilarete.stalactite.spring.repository.query",
		// because we have another repository in the same package, we filter them to keep only the appropriate one (it also checks that filtering works !)
		includeFilters = @Filter(
				type = FilterType.ASSIGNABLE_TYPE,
				classes = DerivedQueriesWithoutMappedCollectionRepository.class)
)
abstract class AbstractDerivedQueriesWithoutMappedCollectionTest {
	
	@Autowired
	private DerivedQueriesWithoutMappedCollectionRepository derivedQueriesRepository;
	
	@Test
	void limit() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Person president1 = new Person(666);
		president1.setName("me");
		country1.setPresident(president1);
		
		Republic country2 = new Republic(43);
		country2.setName("Tata");
		Person president2 = new Person(237);
		president2.setName("you");
		country2.setPresident(president2);
		
		Republic country3 = new Republic(44);
		country3.setName("Titi");
		
		Vehicle vehicle = new Vehicle(1438L);
		vehicle.setColor(new Color(123));
		president1.setVehicle(vehicle);
		
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3));

		Republic loadedCountry = derivedQueriesRepository.findFirstByOrderByNameAsc();
		assertThat(loadedCountry).isEqualTo(country2);
		
		Set<Republic> loadedCountries = derivedQueriesRepository.findTop2ByOrderByNameAsc();
		assertThat(loadedCountries).containsExactly(country2, country3);
	}
	
	@Test
	void projection_resultIsSingle() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Person president1 = new Person(666);
		president1.setName("John Do");
		country1.setPresident(president1);
		Republic country2 = new Republic(43);
		country2.setName("Tata");
		Person president2 = new Person(777);
		president2.setName("Jane Do");
		country2.setPresident(president2);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));

		// test case of a single result to ensure that the algorithm of projection handle it as well as collections
		NamesOnly loadedCountries = derivedQueriesRepository.getByName("Tata");
		assertThat(loadedCountries).extracting(NamesOnly::getName)
				.isEqualTo(country2.getName());
		assertThat(loadedCountries).extracting(chain(NamesOnly::getPresident, NamesOnly.SimplePerson::getName))
				.isEqualTo(country2.getPresident().getName());
	}
	
	@Test
	void projection_byExtraArgument() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Person president1 = new Person(666);
		president1.setName("John Do");
		country1.setPresident(president1);
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		Person president2 = new Person(777);
		president2.setName("Jane Do");
		country2.setPresident(president2);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2));

		// case of a projection as a dynamic type as argument
		Collection<NamesOnlyWithValue> loadedCountries = derivedQueriesRepository.getByName("Toto", NamesOnlyWithValue.class);
		assertThat(loadedCountries).extracting(NamesOnly::getName)
				.containsExactlyInAnyOrder(country1.getName(), country2.getName());
		assertThat(loadedCountries).extracting(NamesOnlyWithValue::getPresidentName)
				.containsExactlyInAnyOrder(
						country1.getPresident().getName() + "-" + country1.getPresident().getId().getDelegate(),
						country2.getPresident().getName() + "-" + country2.getPresident().getId().getDelegate()
				);
		assertThat(loadedCountries).extracting(chain(NamesOnly::getPresident, NamesOnly.SimplePerson::getName))
				.containsExactlyInAnyOrder(country1.getPresident().getName(), country2.getPresident().getName());
		
		// special case where argument is the actual domain type or one super-type
		Collection<AbstractCountry> loadedEntities = derivedQueriesRepository.getByName("Toto", AbstractCountry.class);
		assertThat(loadedEntities).containsExactlyInAnyOrder(country1, country2);
	}
	
	@Test
	void projection_orderBy() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Person president1 = new Person(666);
		president1.setName("Me");
		country1.setPresident(president1);
		Republic country2 = new Republic(43);
		country2.setName("Titi");
		Person president2 = new Person(667);
		president2.setName("John Do");
		country2.setPresident(president2);
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		Person president3 = new Person(668);
		president3.setName("Jane Do");
		country3.setPresident(president3);
		Republic country4 = new Republic(45);
		country4.setName("Tonton");
		Person president4 = new Person(669);
		president4.setName("Saca do");
		country4.setPresident(president4);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));
		
		Set<NamesOnly> loadedNamesOnly = derivedQueriesRepository.getByNameLikeOrderByPresidentNameAsc("%o%");
		assertThat(loadedNamesOnly).extracting(NamesOnly::getName).containsExactly(country1.getName(), country4.getName());
	}
	
	@Test
	void projection_pageable() {
		Republic country1 = new Republic(42);
		country1.setName("Toto");
		Person president1 = new Person(666);
		president1.setName("Me");
		country1.setPresident(president1);
		Republic country2 = new Republic(43);
		country2.setName("Titi");
		Person president2 = new Person(667);
		president2.setName("John Do");
		country2.setPresident(president2);
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		Person president3 = new Person(668);
		president3.setName("Jane Do");
		country3.setPresident(president3);
		Republic country4 = new Republic(45);
		country4.setName("Tonton");
		Person president4 = new Person(669);
		president4.setName("Saca do");
		country4.setPresident(president4);
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4));
		
		PageRequest pageable = PageRequest.ofSize(3);
		Slice<NamesOnly> loadedNamesOnly2;
		loadedNamesOnly2 = derivedQueriesRepository.getByNameLikeOrderByPresidentNameAsc("%t%", pageable);
		assertThat(loadedNamesOnly2).extracting(NamesOnly::getName)
				.containsExactly(country3.getName(), country2.getName(), country1.getName());

		loadedNamesOnly2 = derivedQueriesRepository.getByNameLikeOrderByPresidentNameAsc("%t%", pageable.next());
		assertThat(loadedNamesOnly2).extracting(NamesOnly::getName)
				.containsExactly(country4.getName());
	}
	
	@Test
	void pageable() {
		Republic country1 = new Republic(42);
		country1.setName("Titi");
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		Republic country4 = new Republic(45);
		country4.setName("Tutu");
		Republic country5 = new Republic(46);
		country5.setName("Tonton");
		Republic country6 = new Republic(47);
		country6.setName("TinTin");
		Republic country7 = new Republic(48);
		country7.setName("Toutou");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4, country5, country6, country7));
		
		Page<Republic> loadedCountries;
		
		loadedCountries = derivedQueriesRepository.findByNameLikeOrderByIdAsc("%T%", PageRequest.ofSize(2));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(4);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(7);
		assertThat(loadedCountries.get()).containsExactly(country1, country2);
		
		PageRequest pageable = PageRequest.ofSize(2);
		loadedCountries = derivedQueriesRepository.findByNameLikeOrderByIdAsc("T%o%", pageable);
		assertThat(loadedCountries.getTotalPages()).isEqualTo(2);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(3);
		assertThat(loadedCountries.get()).containsExactly(country2, country5);
		
		loadedCountries = derivedQueriesRepository.findByNameLikeOrderByIdAsc("T%o%", pageable.next());
		assertThat(loadedCountries.getTotalPages()).isEqualTo(2);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(3);
		assertThat(loadedCountries.get()).containsExactly(country7);
		
		loadedCountries = derivedQueriesRepository.findByNameLikeOrderByIdAsc("%T%", PageRequest.of(1, 2));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(4);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(7);
		assertThat(loadedCountries.get()).containsExactly(country3, country4);
		
		loadedCountries = derivedQueriesRepository.findByNameLikeOrderByIdAsc("T%o%", PageRequest.of(1, 2));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(2);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(3);
		assertThat(loadedCountries.get()).containsExactly(country7);
	}
	
	@Test
	void pageable_withOrder() {
		Republic country1 = new Republic(42);
		country1.setName("Titi");
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		Republic country4 = new Republic(45);
		country4.setName("Tutu");
		Republic country5 = new Republic(46);
		country5.setName("Tonton");
		Republic country6 = new Republic(47);
		country6.setName("TinTin");
		Republic country7 = new Republic(48);
		country7.setName("Toutou");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4, country5, country6, country7));
		
		Page<Republic> loadedCountries;
		
		loadedCountries = derivedQueriesRepository.findByNameLike("%T%", PageRequest.ofSize(2).withSort(by("id").ascending()));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(4);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(7);
		assertThat(loadedCountries.get()).containsExactly(country1, country2);
		
		loadedCountries = derivedQueriesRepository.findByNameLike("T%o%", PageRequest.ofSize(2).withSort(by("id").ascending()));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(2);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(3);
		assertThat(loadedCountries.get()).containsExactly(country2, country5);
		
		loadedCountries = derivedQueriesRepository.findByNameLike("%T%", PageRequest.of(1, 2).withSort(by("id").ascending()));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(4);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(7);
		assertThat(loadedCountries.get()).containsExactly(country3, country4);
		
		loadedCountries = derivedQueriesRepository.findByNameLike("T%o%", PageRequest.of(1, 2).withSort(by("id").ascending()));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(2);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(3);
		assertThat(loadedCountries.get()).containsExactly(country7);
		
	}
	
	@Test
	void pageable_withCompositeOrder() {
		Republic country1 = new Republic(42);
		country1.setName("Titi");
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		Republic country4 = new Republic(45);
		country4.setName("Tata");
		Republic country5 = new Republic(46);
		country5.setName("Tata");
		Republic country6 = new Republic(47);
		country6.setName("Titi");
		Republic country7 = new Republic(48);
		country7.setName("Toto");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4, country5, country6, country7));
		
		Page<Republic> loadedCountries;
		
		loadedCountries = derivedQueriesRepository.findByNameLike("T%o", PageRequest.of(1, 2).withSort(by("id").ascending()));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(1);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(2);
		assertThat(loadedCountries.get()).isEmpty();
		
		loadedCountries = derivedQueriesRepository.findByNameLike("%T%", PageRequest.ofSize(2)
				.withSort(by("name").ascending().and(by("id").ascending())));
		assertThat(loadedCountries.getTotalPages()).isEqualTo(4);
		assertThat(loadedCountries.getTotalElements()).isEqualTo(7);
		assertThat(loadedCountries.get()).containsExactly(country3, country4);
	}
	
	@Test
	void slice() {
		Republic country1 = new Republic(42);
		country1.setName("Titi");
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		Republic country4 = new Republic(45);
		country4.setName("Tutu");
		Republic country5 = new Republic(46);
		country5.setName("Tonton");
		Republic country6 = new Republic(47);
		country6.setName("TinTin");
		Republic country7 = new Republic(48);
		country7.setName("Toutou");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4, country5, country6, country7));
		
		Slice<Republic> loadedCountries;
		
		loadedCountries = derivedQueriesRepository.searchByNameLikeOrderByIdAsc("%T%");
		assertThat(loadedCountries.get()).containsExactly(country1, country2, country3, country4, country5, country6, country7);
		assertThat(loadedCountries.hasNext()).isFalse();
		
		loadedCountries = derivedQueriesRepository.searchByNameLikeOrderByIdAsc("%T%", PageRequest.ofSize(2));
		assertThat(loadedCountries.get()).containsExactly(country1, country2);
		assertThat(loadedCountries.getContent()).containsExactly(country1, country2);
		assertThat(loadedCountries.hasNext()).isTrue();
		assertThat(loadedCountries.nextPageable()).isEqualTo(PageRequest.of(1, 2));
		
		loadedCountries = derivedQueriesRepository.searchByNameLikeOrderByIdAsc("T%o%", PageRequest.of(1, 2));
		assertThat(loadedCountries.get()).containsExactly(country7);
		assertThat(loadedCountries.nextPageable()).isEqualTo(Pageable.unpaged());
	}
	
	@Test
	void slice_withOrder() {
		Republic country1 = new Republic(42);
		country1.setName("Titi");
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		Republic country4 = new Republic(45);
		country4.setName("Tutu");
		Republic country5 = new Republic(46);
		country5.setName("Tonton");
		Republic country6 = new Republic(47);
		country6.setName("TinTin");
		Republic country7 = new Republic(48);
		country7.setName("Toutou");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4, country5, country6, country7));
		
		Slice<Republic> loadedCountries = derivedQueriesRepository.searchByNameLike("%T%", PageRequest.ofSize(2).withSort(by("id").ascending()));
		assertThat(loadedCountries.get()).containsExactly(country1, country2);
		assertThat(loadedCountries.getContent()).containsExactly(country1, country2);
		assertThat(loadedCountries.hasNext()).isTrue();
		assertThat(loadedCountries.nextPageable()).isEqualTo(PageRequest.of(1, 2).withSort(by("id").ascending()));
	}
	
	@Test
	void stream() {
		Republic country1 = new Republic(42);
		country1.setName("Titi");
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		Republic country4 = new Republic(45);
		country4.setName("Tutu");
		Republic country5 = new Republic(46);
		country5.setName("Tonton");
		Republic country6 = new Republic(47);
		country6.setName("TinTin");
		Republic country7 = new Republic(48);
		country7.setName("Toutou");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4, country5, country6, country7));
		
		Stream<Republic> loadedCountries;
		List<Republic> collectedResult;
		
		loadedCountries = derivedQueriesRepository.streamByNameLikeOrderByIdAsc("%T%");
		collectedResult = loadedCountries.collect(Collectors.toList());
		assertThat(collectedResult).containsExactly(country1, country2, country3, country4, country5, country6, country7);
		
		loadedCountries = derivedQueriesRepository.streamByNameLikeOrderByIdAsc("%T%", PageRequest.ofSize(2));
		collectedResult = loadedCountries.collect(Collectors.toList());
		assertThat(collectedResult).containsExactly(country1, country2);
		
		loadedCountries = derivedQueriesRepository.streamByNameLikeOrderByIdAsc("T%o%", PageRequest.of(1, 2));
		collectedResult = loadedCountries.collect(Collectors.toList());
		assertThat(collectedResult).containsExactly(country7);
	}
	
	@Test
	void dynamicSort() {
		Republic country1 = new Republic(42);
		country1.setName("Titi");
		Republic country2 = new Republic(43);
		country2.setName("Toto");
		Republic country3 = new Republic(44);
		country3.setName("Tata");
		Republic country4 = new Republic(45);
		country4.setName("Tata");
		Republic country5 = new Republic(46);
		country5.setName("Tata");
		Republic country6 = new Republic(47);
		country6.setName("Titi");
		Republic country7 = new Republic(48);
		country7.setName("Toto");
		derivedQueriesRepository.saveAll(Arrays.asList(country1, country2, country3, country4, country5, country6, country7));
		
		Set<Republic> loadedCountries;
		
		loadedCountries = derivedQueriesRepository.findByNameLike("T%o", by("id").descending());
		assertThat(loadedCountries).containsExactly(country7, country2);
		
		loadedCountries = derivedQueriesRepository.findByNameLike("%T%", by("name").ascending().and(by("id").ascending()));
		assertThat(loadedCountries).containsExactly(country3, country4, country5, country1, country6, country2, country7);
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
			Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
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
			DDLDeployer ddlDeployer = new DDLDeployer(dialect.getDdlTableGenerator(), dialect.getDdlSequenceGenerator(), new CurrentThreadTransactionalConnectionProvider(dataSource));
			ddlDeployer.getDdlGenerator().addTables(DDLDeployer.collectTables(persistenceContext));
			ddlDeployer.deployDDL();
		}
	}
}
