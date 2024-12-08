package org.codefilarete.stalactite.spring.repository.query.bean;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.stalactite.engine.EntityPersister.ExecutableEntityQuery;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.collection.Arrays;
import org.springframework.data.relational.repository.query.RelationalParameters.RelationalParameter;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;

public class BeanRepositoryQuery<C> implements RepositoryQuery {
	
	private static final String PARAMETER_NEEDS_TO_BE_NAMED = "For queries with named parameters you need to provide names for method parameters. Use @Param for query method parameters, or when on Java 8+ use the javac flag -parameters.";
	
	private final BeanQueryMethod queryMethod;
	private final ExecutableEntityQuery<C, ?> sql;
	private final AdvancedEntityPersister<C, ?> entityPersister;
	private final Accumulator<C, ?, ?> accumulator;
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	
	public BeanRepositoryQuery(BeanQueryMethod queryMethod,
							   ExecutableEntityQuery<C, ?> sql,
							   AdvancedEntityPersister<C, ?> entityPersister,
							   Accumulator<C, ?, ?> accumulator,
							   Dialect dialect,
							   ConnectionProvider connectionProvider) {
		this.queryMethod = queryMethod;
		this.sql = sql;
		this.entityPersister = entityPersister;
		this.accumulator = accumulator;
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
		
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
	public BeanQueryMethod getQueryMethod() {
		return queryMethod;
	}
	
	@Override
	public Object execute(Object[] parameters) {
		ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
		
		getValues(accessor).forEach(sql::set);
		
		return sql.execute(accumulator);
	}
	
	private Map<String, Object> getValues(ParametersParameterAccessor accessor) {
		Map<String, Object> result = new HashMap<>();
		for (RelationalParameter bindableParameter : queryMethod.getParameters().getBindableParameters()) {
			String parameterName = bindableParameter.getName().orElseThrow(() -> new IllegalStateException(PARAMETER_NEEDS_TO_BE_NAMED));
			Object value = accessor.getBindableValue(bindableParameter.getIndex());
			if (value.getClass().isArray()) {
				Object[] values = (Object[]) value;
				if (values.length == 1) {
					value = values[0];
				} else {
					value = Arrays.asList(values);
				}
			}
			result.put(parameterName, value);
		}
		return result;
	}
}
