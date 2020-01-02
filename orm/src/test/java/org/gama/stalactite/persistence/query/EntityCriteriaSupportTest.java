package org.gama.stalactite.persistence.query;

import java.util.HashMap;

import org.gama.lang.collection.ValueFactoryMap;
import org.gama.lang.test.Assertions;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.IEntityConfiguredPersister;
import org.gama.stalactite.persistence.engine.IEntityPersister.EntityCriteria;
import org.gama.stalactite.persistence.engine.MappingEase;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.RuntimeMappingException;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * @author Guillaume Mary
 */
class EntityCriteriaSupportTest {
	
	@Test
	void apiUsage() {
		
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "bigint");
		dialect.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identified.class, "bigint");
		
		IEntityConfiguredPersister<Country, Long> persister = MappingEase.entityBuilder(Country.class, long.class)
				.add(Country::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.add(Country::getName)
				.addOneToOne(Country::getCapital, MappingEase.entityBuilder(City.class, long.class)
						.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
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
		
		WhereBuilder queryBuilder = new WhereBuilder(((EntityCriteriaSupport) countryEntityCriteriaSupport).getCriteria(), new DMLNameProvider(new ValueFactoryMap<>(new HashMap<>(), Table::getName)));
		System.out.println(queryBuilder.toSQL());
	}
	
	@Test
	void graphNode_getColumn() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getColumnBinderRegistry().register((Class) Identified.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		Column nameColumn = countryTable.addColumn("name", String.class);
		IEntityMappingStrategy<Country, Long, ?> mappingStrategy = MappingEase.entityBuilder(Country.class, long.class)
				.add(Country::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.add(Country::getName)
				.build(dummyPersistenceContext, countryTable)
				.getMappingStrategy();
		
		EntityGraphNode testInstance = new EntityGraphNode(mappingStrategy);
		assertEquals(nameColumn, testInstance.getColumn(new AccessorByMethodReference<>(Country::getName)));
	}
	
	@Test
	void graphNode_getColumn_withRelationOneToOne() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "bigint");
		dialect.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identified.class, "bigint");
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		Table cityTable = new Table("City");
		Column nameColumn = cityTable.addColumn("name", String.class);
		IEntityMappingStrategy<Country, Long, ?> mappingStrategy = MappingEase.entityBuilder(Country.class, long.class)
				.add(Country::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.add(Country::getName)
				.addOneToOne(Country::getCapital, MappingEase.entityBuilder(City.class, long.class)
						.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.add(City::getName), cityTable)
				.build(dummyPersistenceContext, countryTable)
				.getMappingStrategy();
		
		// we have to register the relation, that is expected by EntityGraphNode
		EntityGraphNode testInstance = new EntityGraphNode(mappingStrategy);
		testInstance.registerRelation(new AccessorByMethodReference<>(Country::getCapital),
				((IEntityConfiguredPersister) dummyPersistenceContext.getPersister(City.class)).getMappingStrategy());
		assertEquals(nameColumn, testInstance.getColumn(new AccessorByMethodReference<>(Country::getCapital), new AccessorByMethodReference<>(City::getName)));
	}
	
	@Test
	void graphNode_getColumn_withRelationOneToMany() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		Table cityTable = new Table("City");
		Column nameColumn = cityTable.addColumn("name", String.class);
		IEntityMappingStrategy<Country, Long, ?> mappingStrategy = MappingEase.entityBuilder(Country.class, long.class)
				.add(Country::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.add(Country::getName)
				.addOneToManySet(Country::getCities, MappingEase.entityBuilder(City.class, long.class)
						.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.add(City::getName), cityTable
				)
				.build(dummyPersistenceContext, countryTable)
				.getMappingStrategy();
		
		// we have to register the relation, that is expected by EntityGraphNode
		EntityGraphNode testInstance = new EntityGraphNode(mappingStrategy);
		testInstance.registerRelation(new AccessorByMethodReference<>(Country::getCities),
				((IEntityConfiguredPersister) dummyPersistenceContext.getPersister(City.class)).getMappingStrategy());
		assertEquals(nameColumn, testInstance.getColumn(new AccessorByMethodReference<>(Country::getCities), new AccessorByMethodReference<>(City::getName)));
	}
	
	@Test
	void graphNode_getColumn_doesntExist_throwsException() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		IEntityMappingStrategy<Country, Long, ?> mappingStrategy = MappingEase.entityBuilder(Country.class, long.class)
				.add(Country::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.build(dummyPersistenceContext, countryTable)
				.getMappingStrategy();
		
		EntityGraphNode testInstance = new EntityGraphNode(mappingStrategy);
		Assertions.assertThrows(() -> testInstance.getColumn(new AccessorByMethodReference<>(Country::getName)),
				Assertions.hasExceptionInCauses(RuntimeMappingException.class).andProjection(Assertions.hasMessage("Column for Country::getName was not found")));
	}
	
	@Test
	void graphNode_getColumn_many_doesntExist_throwsException() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		IEntityMappingStrategy<Country, Long, ?> mappingStrategy = MappingEase.entityBuilder(Country.class, long.class)
				.add(Country::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.build(dummyPersistenceContext, countryTable)
				.getMappingStrategy();
		
		EntityCriteriaSupport<Country> testInstance = new EntityCriteriaSupport<>(mappingStrategy);
		Assertions.assertThrows(() -> testInstance.andMany(Country::getCities, City::getName, Operators.eq("Grenoble")),
				Assertions.hasExceptionInCauses(RuntimeMappingException.class).andProjection(Assertions.hasMessage("Column for Country::getCities > City::getName was not found")));
	}
}