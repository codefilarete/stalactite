package org.gama.stalactite.persistence.query;

import org.gama.sql.ConnectionProvider;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupport;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.QueryBuilder;
import org.gama.stalactite.query.model.Operator;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;

/**
 * @author Guillaume Mary
 */
class EntityCriteriaTest {
	
	@Test
	void apiUsage() {
		
		Persister<Country, Long, Table> persister = FluentEntityMappingConfigurationSupport.from(Country.class, long.class)
				.add(Country::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.add(Country::getName)
				.addOneToOne(Country::getCapital, FluentEntityMappingConfigurationSupport.from(City.class, long.class)
						.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
						.add(City::getName)
						.getConfiguration()
				)
				.build(new PersistenceContext(mock(ConnectionProvider.class), new Dialect()));
		
		EntityCriteria<Country> countryEntityCriteriaSupport = new EntityCriteriaSupport<>(persister.getMappingStrategy(), Country::getName, Operator.eq(""))
				.and(Country::getId, Operator.in("11"))
				.and(Country::getName, Operator.eq("toto"))
				.and(Country::getName, Operator.between("11", ""))
				.and(Country::getName, Operator.gteq("11"))
				.and(Country::setName, Operator.in("11"))
				.and(Country::setName, Operator.between("11", ""))
				.and(Country::setName, Operator.gteq("11"))
				.and(Country::setCapital, City::getName, Operator.gteq("11"))
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