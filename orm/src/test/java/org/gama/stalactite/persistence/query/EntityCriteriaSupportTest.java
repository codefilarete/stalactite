package org.gama.stalactite.persistence.query;

import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupport;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.QueryBuilder;
import org.gama.stalactite.query.model.Operator;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;

/**
 * @author Guillaume Mary
 */
class EntityCriteriaSupportTest {
	
	@Test
	void apiUsage() {
		
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getColumnBinderRegistry().register((Class) Identified.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		
		JoinedTablesPersister<Country, Long, Table> persister = (JoinedTablesPersister<Country, Long, Table>) FluentEntityMappingConfigurationSupport.from(Country.class, long.class)
				.add(Country::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.add(Country::getName)
				.addOneToOne(Country::getCapital, FluentEntityMappingConfigurationSupport.from(City.class, long.class)
						.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.add(City::getName)
						.getConfiguration()
				)
				.build(new PersistenceContext(mock(ConnectionProvider.class), dialect));
		
		EntityCriteria<Country> countryEntityCriteriaSupport = new EntityCriteriaSupport<>(persister.getJoinedStrategiesSelectExecutor(), Country::getName, Operator.eq(""))
				.and(Country::getId, Operator.in("11"))
				.and(Country::getName, Operator.eq("toto"))
				.and(Country::getName, Operator.between("11", ""))
				.and(Country::getName, Operator.gteq("11"))
				.and(Country::setName, Operator.in("11"))
				.and(Country::setName, Operator.between("11", ""))
				.and(Country::setName, Operator.gteq("11"))
//				.and(Country::setCapital, City::getName, Operator.gteq("11"))
				.or(Country::getId, Operator.in("11"))
				.or(Country::getName, Operator.eq("toto"))
				.or(Country::getName, Operator.between("11", ""))
				.or(Country::getName, Operator.gteq("11"))
				.or(Country::setName, Operator.in("11"))
				.or(Country::setName, Operator.between("11", ""))
				.or(Country::setName, Operator.gteq("11"))
				;
		
		QueryBuilder queryBuilder = new QueryBuilder(((EntityCriteriaSupport) countryEntityCriteriaSupport).getQuery());
		System.out.println(queryBuilder.toSQL());
	}
	
}