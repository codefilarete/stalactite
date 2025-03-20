package org.codefilarete.stalactite.engine.runtime;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.engine.FluentEntityMappingBuilder.FluentMappingBuilderPropertyOptions;
import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.stalactite.engine.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.runtime.EntityCriteriaSupport.EntityGraphNode;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.exception.Exceptions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
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
		
		ConfiguredPersister<Country, Identifier<Long>> persister = (ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, IdentifierPolicy.databaseAutoIncrement())
				.map(Country::getName)
				.mapOneToOne(Country::getCapital, MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
						.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(City::getName))
				.build(new PersistenceContext(mock(ConnectionProvider.class), dialect));
		
		// Note that this constructor can't accept relation as criteria, else we should register relations on it
		// which can be quite difficult because we don't have access to internal objects behind persister variable 
		EntityCriteria<Country, ?> countryEntityCriteriaSupport = new EntityCriteriaSupport<>(persister.getMapping())
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
	void graphNode_getColumn() {
		Dialect dialect = new DefaultDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		Column nameColumn = countryTable.addColumn("name", String.class);
		EntityMapping<Country, Identifier<Long>, ?> mappingStrategy =
				((ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE, countryTable)
				.mapKey(Country::getId, IdentifierPolicy.databaseAutoIncrement())
				.map(Country::getName)
				.build(dummyPersistenceContext))
				.getMapping();
		
		EntityGraphNode<Country> testInstance = new EntityGraphNode<>(mappingStrategy);
		assertThat(testInstance.giveColumn(AccessorChain.chain(Country::getName).getAccessors())).isEqualTo(nameColumn);
	}
	
	@Test
	void graphNode_getColumn_withRelationOneToOne() {
		Dialect dialect = new DefaultDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "bigint");
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		Table cityTable = new Table("City");
		Column nameColumn = cityTable.addColumn("name", String.class);
		
		FluentMappingBuilderPropertyOptions<City, Identifier<Long>, String> cityPersisterConfiguration = MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE, cityTable)
				.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(City::getName);
		
		EntityMapping<Country, Identifier<Long>, ?> mappingStrategy =
				((ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE, countryTable)
				.mapKey(Country::getId, IdentifierPolicy.databaseAutoIncrement())
				.map(Country::getName)
				.mapOneToOne(Country::getCapital, cityPersisterConfiguration)
				.build(dummyPersistenceContext))
				.getMapping();
		
		EntityPersister<City, Identifier<Long>> cityPersister = cityPersisterConfiguration.build(dummyPersistenceContext);
		
		// we have to register the relation, that is expected by EntityGraphNode
		EntityGraphNode<Country> testInstance = new EntityGraphNode<>(mappingStrategy);
		testInstance.registerRelation(new AccessorByMethodReference<>(Country::getCapital),
				((ConfiguredRelationalPersister) cityPersister));
		
		assertThat(testInstance.giveColumn(AccessorChain.chain(Country::getCapital, City::getName).getAccessors())).isEqualTo(nameColumn);
	}
	
	@Test
	void graphNode_getColumn_withRelationOneToMany() {
		Dialect dialect = new DefaultDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		Table cityTable = new Table("City");
		Column nameColumn = cityTable.addColumn("name", String.class);
		FluentMappingBuilderPropertyOptions<City, Identifier<Long>, String> cityPersisterConfiguration = MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE, cityTable)
				.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(City::getName);
		EntityMapping<Country, Identifier<Long>, ?> mappingStrategy =
				((ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE, countryTable)
				.mapKey(Country::getId, IdentifierPolicy.databaseAutoIncrement())
				.map(Country::getName)
				.mapOneToMany(Country::getCities, cityPersisterConfiguration)
				.build(dummyPersistenceContext))
				.getMapping();
		
		EntityPersister<City, Identifier<Long>> cityPersister = cityPersisterConfiguration.build(dummyPersistenceContext);
		
		// we have to register the relation, that is expected by EntityGraphNode
		EntityGraphNode<Country> testInstance = new EntityGraphNode<>(mappingStrategy);
		testInstance.registerRelation(new AccessorByMethodReference<>(Country::getCities),
				((ConfiguredRelationalPersister) cityPersister));
		
		assertThat(testInstance.giveColumn(new AccessorChain<>(
				new AccessorByMethodReference<>(Country::getCities),
				new AccessorByMethodReference<>(City::getName))
				.getAccessors())).isEqualTo(nameColumn);
	}
	
	@Test
	void graphNode_getColumn_doesntExist_throwsException() {
		Dialect dialect = new DefaultDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		EntityMapping<Country, Identifier<Long>, ?> mappingStrategy =
				((ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE, countryTable)
				.mapKey(Country::getId, IdentifierPolicy.databaseAutoIncrement())
				.build(dummyPersistenceContext))
				.getMapping();
		
		EntityGraphNode<Country> testInstance = new EntityGraphNode<>(mappingStrategy);
		assertThatThrownBy(() -> testInstance.giveColumn(AccessorChain.chain(Country::getName).getAccessors()))
				.extracting(t -> Exceptions.findExceptionInCauses(t, MappingConfigurationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Error while looking for column of Country::getName : it is not declared in mapping of o.c.s.e.m.Country");
	}
	
	@Test
	void graphNode_getColumn_many_doesntExist_throwsException() {
		Dialect dialect = new DefaultDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		EntityMapping<Country, Identifier<Long>, ?> mappingStrategy =
				((ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE, countryTable)
				.mapKey(Country::getId, IdentifierPolicy.databaseAutoIncrement())
				.build(dummyPersistenceContext))
				.getMapping();
		
		EntityCriteriaSupport<Country> testInstance = new EntityCriteriaSupport<>(mappingStrategy);
		assertThatThrownBy(() -> testInstance.andMany(Country::getCities, City::getName, Operators.eq("Grenoble")))
				.extracting(t -> Exceptions.findExceptionInCauses(t, MappingConfigurationException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Error while looking for column of Country::getCities > City::getName : it is not declared in mapping of o.c.s.e.m.Country");
	}
	
	@Test
	void hasCollectionCriteria_noCollectionCriteria_returnsFalse() {
		Table personTable = new Table("Person");
		Column nameColumn = personTable.addColumn("name", String.class);
		
		EntityMapping entityMappingMock = mock(EntityMapping.class);
		when(entityMappingMock.getPropertyToColumn()).thenReturn(Maps.forHashMap(ReversibleAccessor.class, Column.class).add(Accessors.accessor(Person::getName), nameColumn));
		EntityCriteriaSupport<Person> testInstance = new EntityCriteriaSupport<Person>(entityMappingMock);
		// first check: when criteria, there's also no collection criteria !
		assertThat(testInstance.hasCollectionCriteria()).isFalse();
		
		testInstance.and(Person::getName, Operators.eq(""));
		assertThat(testInstance.hasCollectionCriteria()).isFalse();
	}
	
	@Test
	void hasCollectionCriteria_embeddedCollectionCriteria_returnsTrue() {
		Table personTable = new Table("Person");
		Column nameColumn = personTable.addColumn("name", String.class);
		
		EntityCriteriaSupport<Person> testInstance = new EntityCriteriaSupport<Person>(mock(EntityMapping.class));
		// we have to register the relation, that is expected by EntityGraphNode
		ConfiguredRelationalPersister<Person, Identifier<Long>> persisterMock = mock(ConfiguredRelationalPersister.class);
		when(persisterMock.getColumn(anyList())).thenReturn(nameColumn);
		testInstance.registerRelation(new AccessorByMethodReference<>(Person::getNicknames), persisterMock);
		testInstance.and(Person::getNicknames, Operators.eq(""));
		assertThat(testInstance.hasCollectionCriteria()).isTrue();
	}
	
	@Test
	void hasCollectionCriteria_manyCriteria_returnsTrue() {
		Table personTable = new Table("Person");
		Column nameColumn = personTable.addColumn("name", String.class);
		
		EntityCriteriaSupport<Country> testInstance = new EntityCriteriaSupport<Country>(mock(EntityMapping.class));
		// we have to register the relation, that is expected by EntityGraphNode
		ConfiguredRelationalPersister<Country, Identifier<Long>> persisterMock = mock(ConfiguredRelationalPersister.class);
		when(persisterMock.getColumn(anyList())).thenReturn(nameColumn);
		testInstance.registerRelation(new AccessorByMethodReference<>(Country::getCities), persisterMock);
		testInstance.andMany(Country::getCities, City::getName, Operators.eq(""));
		assertThat(testInstance.hasCollectionCriteria()).isTrue();
	}
}