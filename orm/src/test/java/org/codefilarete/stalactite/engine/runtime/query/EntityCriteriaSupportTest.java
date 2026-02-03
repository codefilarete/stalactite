package org.codefilarete.stalactite.engine.runtime.query;

import java.util.List;
import java.util.stream.Collectors;

import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.MappingEase;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityCriteria;
import org.codefilarete.stalactite.engine.EntityCriteria.CriteriaPath;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityPersister.ExecutableEntityQueryCriteria;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.GroupBy;
import org.codefilarete.stalactite.query.model.Having;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.LogicalOperator;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Where;
import org.codefilarete.stalactite.query.model.operator.Equals;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class EntityCriteriaSupportTest {
	
	@Test
	void apiUsage() {
		
		Dialect dialect = new DefaultDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "bigint");
		
		RelationalEntityPersister<Country, Identifier<Long>> persister = (RelationalEntityPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, IdentifierPolicy.databaseAutoIncrement())
				.map(Country::getName)
				.mapOneToOne(Country::getCapital, MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
						.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(City::getName))
				.build(new PersistenceContext(mock(ConnectionProvider.class), dialect));
		
		// Note that this constructor can't accept relation as criteria, else we should register relations on it
		// which can be quite difficult because we don't have access to internal objects behind persister variable 
		EntityCriteria<Country, ?> countryEntityCriteriaSupport = new EntityCriteriaSupport<>(persister.getEntityJoinTree(), true)
				.and(Country::getId, Operators.in("11"))
				.and(Country::getName, Operators.eq("toto"))
				.and(Country::getName, Operators.between("11", ""))
				.and(Country::getName, Operators.gteq("11"))
				.and(Country::setName, Operators.in("11"))
				.and(Country::setName, Operators.between("11", ""))
				.and(Country::setName, Operators.gteq("11"))
				.or(Country::getId, Operators.in("11"))
				.or(Country::getName, Operators.eq("toto"))
				.or(Country::getName, Operators.between("11", ""))
				.or(Country::getName, Operators.gteq("11"))
				.or(Country::setName, Operators.in("11"))
				.or(Country::setName, Operators.between("11", ""))
				.or(Country::setName, Operators.gteq("11"))
				;
		
		persister.selectWhere(Country::getCapital, City::getId, Operators.gteq("11"))
				.and(Country::getCapital, City::getId, Operators.gteq("11"));
	}
	
	@Test
	void sqlGeneration() {
		Dialect dialect = new DefaultDialect();
		
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "bigint");
		
		FluentEntityMappingBuilder.FluentMappingBuilderPropertyOptions<City, Identifier<Long>, String> map = entityBuilder(City.class, LONG_TYPE)
				.mapKey(City::getId, ALREADY_ASSIGNED)
				.map(City::getName);
		RelationalEntityPersister<Country, Identifier<Long>> persister = (RelationalEntityPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, IdentifierPolicy.databaseAutoIncrement())
				.mapOneToOne(Country::getCapital, map)
				.mapOneToOne(Country::getPresident, entityBuilder(Person.class, Identifier.LONG_TYPE)
						.mapKey(Person::getId, ALREADY_ASSIGNED)
						.mapCollection(Person::getNicknames, String.class)
				)
				.mapOneToMany(Country::getCities, map)
				.build(new PersistenceContext(mock(ConnectionProvider.class), dialect));
		
		EntityCriteriaSupport<Country> testInstance = new EntityCriteriaSupport<>(persister.getEntityJoinTree(), true);
		testInstance
				.and(Country::getCapital, City::getId, Operators.gteq("11"))
				.and(Country::getPresident, Person::getNicknames, Operators.gteq("11"))
				.and(new CriteriaPath<>(Country::getCities, City::getId), Operators.isNull())
		;
		
		EntityTreeQuery<Country> entityTreeQuery = new EntityTreeQueryBuilder<>(persister.getEntityJoinTree(), dialect.getColumnBinderRegistry()).buildSelectQuery();
		
		Query queryClone = new Query(
				entityTreeQuery.getQuery().getSelectDelegate(),
				entityTreeQuery.getQuery().getFromDelegate(),
				new Where<>(testInstance.getCriteria()),
				new GroupBy(),
				new Having(),
				new OrderBy(),
				new Limit());
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(queryClone);
		String sql = sqlQueryBuilder.toSQL();
		assertThat(sql).isEqualTo("select"
				+ " Country.id as Country_id,"
				+ " capital.name as capital_name,"
				+ " capital.id as capital_id,"
				+ " president.id as president_id,"
				+ " president_Person_nicknames.nicknames as president_Person_nicknames_nicknames,"
				+ " president_Person_nicknames.id as president_Person_nicknames_id,"
				+ " Country_cities_City.name as Country_cities_City_name,"
				+ " Country_cities_City.id as Country_cities_City_id"
				+ " from Country"
				+ " left outer join City as capital on Country.capitalId = capital.id"
				+ " left outer join Person as president on Country.presidentId = president.id"
				+ " left outer join Country_cities as Country_cities on Country.id = Country_cities.country_id"
				+ " left outer join Person_nicknames as president_Person_nicknames on president.id = president_Person_nicknames.id"
				+ " left outer join City as Country_cities_City on Country_cities.cities_id = Country_cities_City.id"
				+ " where capital.id >= '11'"
				+ " and president_Person_nicknames.nicknames >= '11'"
				+ " and Country_cities_City.id is null");
	}
	
	@Test
	void hasCollectionCriteria_noCollectionCriteria_returnsFalse() {
		Table personTable = new Table("Person");
		Column nameColumn = personTable.addColumn("name", String.class);
		
		EntityMapping entityMappingMock = mock(EntityMapping.class);
		when(entityMappingMock.getPropertyToColumn()).thenReturn(Maps.forHashMap(ReversibleAccessor.class, Column.class).add(Accessors.accessor(Person::getName), nameColumn));
		// we have to mock the identifier mapping because its columns are collected as eventual criteria (else a NPE is thrown)
		when(entityMappingMock.getIdMapping()).thenReturn(mock(IdMapping.class));
		when(entityMappingMock.getTargetTable()).thenReturn(personTable);
		EntityCriteriaSupport<Person> testInstance = new EntityCriteriaSupport<>(new EntityJoinTree<Person, Object>(entityMappingMock), true);
		// first check: when there's no criteria, there's also no collection criteria !
		assertThat(testInstance.hasCollectionCriteria()).isFalse();
		
		testInstance.and(Person::getName, Operators.eq(""));
		assertThat(testInstance.hasCollectionCriteria()).isFalse();
	}
	
	@Test
	void hasCollectionCriteria_embeddedCollectionCriteria_returnsTrue() {
		Table personTable = new Table("Person");
		Column nameColumn = personTable.addColumn("name", String.class);
		
		EntityMapping entityMappingMock = mock(EntityMapping.class);
		when(entityMappingMock.getPropertyToColumn()).thenReturn(Maps.forHashMap(ReversibleAccessor.class, Column.class).add(Accessors.accessor(Person::getNicknames), nameColumn));
		// we have to mock the identifier mapping because its columns are collected as eventual criteria (else a NPE is thrown)
		when(entityMappingMock.getIdMapping()).thenReturn(mock(IdMapping.class));
		when(entityMappingMock.getTargetTable()).thenReturn(personTable);
		EntityCriteriaSupport<Person> testInstance = new EntityCriteriaSupport<Person>(new EntityJoinTree<Person, Object>(entityMappingMock), true) {
			@Override
			void appendAsCriterion(LogicalOperator logicalOperator, List accessPointChain, ConditionalOperator operator) {
				// overridden to do nothing because it's too complex and that's not the goal of our test method
			}
		};
		testInstance.and(Person::getNicknames, Operators.<String>eq(""));
		assertThat(testInstance.hasCollectionCriteria()).isTrue();
	}
	
	/**
	 * Tests the ability to query entities based on criteria that involve elements of a collection property.
	 */
	@Test
	void selectWhere_withCollectionCriteria() {
		
		Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		
		PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), dialect);
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		
		EntityPersister<Country, Identifier<Long>> countryPersister = entityBuilder(Country.class, LONG_TYPE)
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.mapOneToMany(Country::getCities, entityBuilder(City.class, LONG_TYPE)
						.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(City::getName))
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Country country = new Country(new PersistableIdentifier<>(1L));
		City grenoble = new City(new PersistableIdentifier<>(13L));
		grenoble.setName("Grenoble");
		country.addCity(grenoble);
		City lyon = new City(new PersistableIdentifier<>(17L));
		lyon.setName("Lyon");
		country.addCity(lyon);
		countryPersister.insert(country);
		
		// we create a condition using criteria applied to a property of an element within the cities collection
		Country loadedCountry = countryPersister.selectWhere(Country::getCities, City::getName, Operators.eq("Grenoble")).execute(Accumulators.getFirstUnique());
		assertThat(loadedCountry).isNotNull();
		// we ensure that the whole aggregate is loaded, not a subset of the collection
		assertThat(loadedCountry.getCities().stream().map(City::getName).collect(Collectors.toSet())).containsExactlyInAnyOrder("Grenoble", "Lyon");
	}
}
