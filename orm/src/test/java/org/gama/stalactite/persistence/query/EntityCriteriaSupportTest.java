package org.gama.stalactite.persistence.query;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.gama.lang.collection.ValueFactoryHashMap;
import org.gama.lang.exception.Exceptions;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.IEntityPersister.EntityCriteria;
import org.gama.stalactite.persistence.engine.MappingEase;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredPersister;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.StatefullIdentifierAlreadyAssignedIdentifierPolicy;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.query.EntityCriteriaSupport.EntityGraphNode;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.DMLNameProvider;
import org.gama.stalactite.query.builder.WhereBuilder;
import org.gama.stalactite.query.model.Operators;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * @author Guillaume Mary
 */
class EntityCriteriaSupportTest {
	
	@Test
	void apiUsage() {
		
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "bigint");
		
		IEntityConfiguredPersister<Country, Long> persister = MappingEase.entityBuilder(Country.class, long.class)
				.add(Country::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.add(Country::getName)
				.addOneToOne(Country::getCapital, MappingEase.entityBuilder(City.class, long.class)
						.add(City::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.add(City::getName))
				.build(new PersistenceContext(mock(ConnectionProvider.class), dialect));
		
		EntityCriteria<Country> countryEntityCriteriaSupport = new EntityCriteriaSupport<>(persister.getMappingStrategy(), Country::getName, Operators.eq(""))
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
		
		WhereBuilder queryBuilder = new WhereBuilder(((EntityCriteriaSupport) countryEntityCriteriaSupport).getCriteria(), new DMLNameProvider(new ValueFactoryHashMap<>(Table::getName)));
		System.out.println(queryBuilder.toSQL());
	}
	
	@Test
	void graphNode_getColumn() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		Column nameColumn = countryTable.addColumn("name", String.class);
		IEntityMappingStrategy<Country, Long, ?> mappingStrategy = MappingEase.entityBuilder(Country.class, long.class)
				.add(Country::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.add(Country::getName)
				.build(dummyPersistenceContext, countryTable)
				.getMappingStrategy();
		
		EntityGraphNode testInstance = new EntityGraphNode(mappingStrategy);
		assertThat(testInstance.getColumn(new AccessorByMethodReference<>(Country::getName))).isEqualTo(nameColumn);
	}
	
	@Test
	void graphNode_getColumn_withRelationOneToOne() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "bigint");
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		Table cityTable = new Table("City");
		Column nameColumn = cityTable.addColumn("name", String.class);
		IEntityMappingStrategy<Country, Long, ?> mappingStrategy = MappingEase.entityBuilder(Country.class, long.class)
				.add(Country::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.add(Country::getName)
				.addOneToOne(Country::getCapital, MappingEase.entityBuilder(City.class, long.class)
						.add(City::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.add(City::getName), cityTable)
				.build(dummyPersistenceContext, countryTable)
				.getMappingStrategy();
		
		// we have to register the relation, that is expected by EntityGraphNode
		EntityGraphNode testInstance = new EntityGraphNode(mappingStrategy);
		testInstance.registerRelation(new AccessorByMethodReference<>(Country::getCapital),
				((IEntityConfiguredPersister) dummyPersistenceContext.getPersister(City.class)).getMappingStrategy());
		assertThat(testInstance.getColumn(new AccessorByMethodReference<>(Country::getCapital), new AccessorByMethodReference<>(City::getName))).isEqualTo(nameColumn);
	}
	
	@Test
	void graphNode_getColumn_withRelationOneToMany() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		Table cityTable = new Table("City");
		Column nameColumn = cityTable.addColumn("name", String.class);
		IEntityMappingStrategy<Country, Long, ?> mappingStrategy = MappingEase.entityBuilder(Country.class, long.class)
				.add(Country::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.add(Country::getName)
				.addOneToManySet(Country::getCities, MappingEase.entityBuilder(City.class, long.class)
						.add(City::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.add(City::getName), cityTable
				)
				.build(dummyPersistenceContext, countryTable)
				.getMappingStrategy();
		
		// we have to register the relation, that is expected by EntityGraphNode
		EntityGraphNode testInstance = new EntityGraphNode(mappingStrategy);
		testInstance.registerRelation(new AccessorByMethodReference<>(Country::getCities),
				((IEntityConfiguredPersister) dummyPersistenceContext.getPersister(City.class)).getMappingStrategy());
		assertThat(testInstance.getColumn(new AccessorByMethodReference<>(Country::getCities), new AccessorByMethodReference<>(City::getName))).isEqualTo(nameColumn);
	}
	
	@Test
	void graphNode_getColumn_doesntExist_throwsException() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		IEntityMappingStrategy<Country, Long, ?> mappingStrategy = MappingEase.entityBuilder(Country.class, long.class)
				.add(Country::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.build(dummyPersistenceContext, countryTable)
				.getMappingStrategy();
		
		EntityGraphNode testInstance = new EntityGraphNode(mappingStrategy);
		assertThatThrownBy(() -> testInstance.getColumn(new AccessorByMethodReference<>(Country::getName)))
				.extracting(t -> Exceptions.findExceptionInCauses(t, RuntimeException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Column for Country::getName was not found");
	}
	
	@Test
	void graphNode_getColumn_many_doesntExist_throwsException() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		IEntityMappingStrategy<Country, Long, ?> mappingStrategy = MappingEase.entityBuilder(Country.class, long.class)
				.add(Country::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.build(dummyPersistenceContext, countryTable)
				.getMappingStrategy();
		
		EntityCriteriaSupport<Country> testInstance = new EntityCriteriaSupport<>(mappingStrategy);
		assertThatThrownBy(() -> testInstance.andMany(Country::getCities, City::getName, Operators.eq("Grenoble")))
				.extracting(t -> Exceptions.findExceptionInCauses(t, RuntimeException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Column for Country::getCities > City::getName was not found");
	}
}