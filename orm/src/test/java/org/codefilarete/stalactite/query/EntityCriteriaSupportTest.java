package org.codefilarete.stalactite.query;

import java.util.HashMap;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.engine.MappingEase;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.EntityConfiguredPersister;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.query.EntityCriteriaSupport.EntityGraphNode;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.builder.WhereBuilder;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
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
		
		ConfiguredPersister<Country, Identifier<Long>> persister = (ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, IdentifierPolicy.afterInsert())
				.map(Country::getName)
				.mapOneToOne(Country::getCapital, MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
						.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(City::getName))
				.build(new PersistenceContext(mock(ConnectionProvider.class), dialect));
		
		EntityCriteria<Country> countryEntityCriteriaSupport = new EntityCriteriaSupport<>(persister.getMapping(), Country::getName, Operators.eq(""))
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
		
		WhereBuilder queryBuilder = new WhereBuilder(((EntityCriteriaSupport) countryEntityCriteriaSupport).getCriteria(), new DMLNameProvider(new HashMap<Table, String>() {
			@Override
			public String get(Object key) {
				String o = super.get(key);
				if (o == null) {
					o = ((Table) key).getName();
					put(((Table) key), o);
				}
				return o;
			}
		}));
		System.out.println(queryBuilder.toSQL());
	}
	
	@Test
	void graphNode_getColumn() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		Column nameColumn = countryTable.addColumn("name", String.class);
		EntityMapping<Country, Identifier<Long>, ?> mappingStrategy =
				((ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, IdentifierPolicy.afterInsert())
				.map(Country::getName)
				.build(dummyPersistenceContext, countryTable))
				.getMapping();
		
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
		EntityMapping<Country, Identifier<Long>, ?> mappingStrategy =
				((ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, IdentifierPolicy.afterInsert())
				.map(Country::getName)
				.mapOneToOne(Country::getCapital, MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
						.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(City::getName), cityTable)
				.build(dummyPersistenceContext, countryTable))
				.getMapping();
		
		// we have to register the relation, that is expected by EntityGraphNode
		EntityGraphNode testInstance = new EntityGraphNode(mappingStrategy);
		testInstance.registerRelation(new AccessorByMethodReference<>(Country::getCapital),
				((EntityConfiguredPersister) dummyPersistenceContext.getPersister(City.class)).getMapping());
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
		EntityMapping<Country, Identifier<Long>, ?> mappingStrategy =
				((ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, IdentifierPolicy.afterInsert())
				.map(Country::getName)
				.mapOneToManySet(Country::getCities, MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
						.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.map(City::getName), cityTable
				)
				.build(dummyPersistenceContext, countryTable))
				.getMapping();
		
		// we have to register the relation, that is expected by EntityGraphNode
		EntityGraphNode testInstance = new EntityGraphNode(mappingStrategy);
		testInstance.registerRelation(new AccessorByMethodReference<>(Country::getCities),
				((EntityConfiguredPersister) dummyPersistenceContext.getPersister(City.class)).getMapping());
		assertThat(testInstance.getColumn(new AccessorByMethodReference<>(Country::getCities), new AccessorByMethodReference<>(City::getName))).isEqualTo(nameColumn);
	}
	
	@Test
	void graphNode_getColumn_doesntExist_throwsException() {
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		
		PersistenceContext dummyPersistenceContext = new PersistenceContext(mock(ConnectionProvider.class), dialect);
		Table countryTable = new Table("Country");
		EntityMapping<Country, Identifier<Long>, ?> mappingStrategy =
				((ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, IdentifierPolicy.afterInsert())
				.build(dummyPersistenceContext, countryTable))
				.getMapping();
		
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
		EntityMapping<Country, Identifier<Long>, ?> mappingStrategy =
				((ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, IdentifierPolicy.afterInsert())
				.build(dummyPersistenceContext, countryTable))
				.getMapping();
		
		EntityCriteriaSupport<Country> testInstance = new EntityCriteriaSupport<>(mappingStrategy);
		assertThatThrownBy(() -> testInstance.andMany(Country::getCities, City::getName, Operators.eq("Grenoble")))
				.extracting(t -> Exceptions.findExceptionInCauses(t, RuntimeException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Column for Country::getCities > City::getName was not found");
	}
}