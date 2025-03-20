package org.codefilarete.stalactite.spring.repository.query.bean;

import org.codefilarete.stalactite.engine.EntityPersister.ExecutableEntityQuery;
import org.codefilarete.stalactite.spring.repository.query.AbstractRepositoryQuery;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.springframework.data.repository.query.ParametersParameterAccessor;

public class BeanRepositoryQuery<C> extends AbstractRepositoryQuery {
	
	private final ExecutableEntityQuery<C, ?> sql;
	private final Accumulator<C, ?, ?> accumulator;
	
	public BeanRepositoryQuery(BeanQueryMethod queryMethod,
							   ExecutableEntityQuery<C, ?> sql,
							   Accumulator<C, ?, ?> accumulator) {
		super(queryMethod);
		this.sql = sql;
		this.accumulator = accumulator;
		
		if (queryMethod.isSliceQuery()) {
			throw new UnsupportedOperationException(
					"Slice queries are not supported using string-based queries. Offending method: " + queryMethod);
		}
		
		if (queryMethod.isPageQuery()) {
			throw new UnsupportedOperationException(
					"Page queries are not supported using string-based queries. Offending method: " + queryMethod);
		}
		
		// TODO: when upgrading to Spring Data 3.x.y, add an assertion on Limit parameter presence as it's done in StringBasedJdbcQuery
		// https://github.com/spring-projects/spring-data-relational/blob/main/spring-data-jdbc/src/main/java/org/springframework/data/jdbc/repository/query/StringBasedJdbcQuery.java#L176
	}
	
	@Override
	public Object execute(Object[] parameters) {
		ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
		
		getValues(accessor).forEach(sql::set);
		
		return sql.execute(accumulator);
	}
}
