package org.codefilarete.stalactite.spring.repository.query.nativ;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.RelationalEntityFinder;
import org.codefilarete.stalactite.spring.repository.query.AbstractRepositoryQuery;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.springframework.core.ResolvableType;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.relational.repository.query.RelationalParameters.RelationalParameter;
import org.springframework.data.repository.query.ParametersParameterAccessor;

public class SqlNativeRepositoryQuery<C> extends AbstractRepositoryQuery {
	
	private static final String PARAMETER_NEEDS_TO_BE_NAMED = "For queries with named parameters you need to provide names for method parameters. Use @Param for query method parameters, or when on Java 8+ use the javac flag -parameters.";
	
	private final String sql;
	private final Accumulator<C, ?, ?> accumulator;
	private final Dialect dialect;
	private final RelationalEntityFinder<C, ?, ?> relationalEntityFinder;
	
	public SqlNativeRepositoryQuery(NativeQueryMethod queryMethod,
									String sql,
									AdvancedEntityPersister<C, ?> entityPersister,
									Accumulator<C, ?, ?> accumulator,
									Dialect dialect,
									ConnectionProvider connectionProvider) {
		super(queryMethod);
		this.sql = sql;
		this.accumulator = accumulator;
		this.dialect = dialect;
		
		if (queryMethod.isSliceQuery()) {
			throw new UnsupportedOperationException(
					"Slice queries are not supported using string-based queries. Offending method: " + queryMethod);
		}
		
		if (queryMethod.isPageQuery()) {
			throw new UnsupportedOperationException(
					"Page queries are not supported using string-based queries. Offending method: " + queryMethod);
		}
		
		// Note that at this stage we can afford to ask for immediate Query creation because we are at a high layer (Spring Data Query discovery) and
		// persister is supposed to be finalized and up-to-date (containing the whole entity aggregate graph), that why we pass "true" as argument
		this.relationalEntityFinder = new RelationalEntityFinder<>(
				entityPersister.getEntityJoinTree(),
				connectionProvider,
				dialect,
				true);
		
		// TODO: when upgrading to Spring Data 3.x.y, add an assertion on Limit parameter presence as it's done in StringBasedJdbcQuery
		// https://github.com/spring-projects/spring-data-relational/blob/main/spring-data-jdbc/src/main/java/org/springframework/data/jdbc/repository/query/StringBasedJdbcQuery.java#L176
	}
	
	@Override
	public NativeQueryMethod getQueryMethod() {
		return (NativeQueryMethod) super.getQueryMethod();
	}
	
	@Override
	public Object execute(Object[] parameters) {
		ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
		return accumulator.collect(relationalEntityFinder.selectFromQueryBean(sql, getValues(accessor), bindParameters(accessor)));
	}
	
	private Map<String, PreparedStatementWriter<?>> bindParameters(ParametersParameterAccessor accessor) {
		Map<String, PreparedStatementWriter<?>> result = new HashMap<>();
		RelationalParameters bindableParameters = getQueryMethod().getParameters().getBindableParameters();
		
		for (RelationalParameter bindableParameter : bindableParameters) {
			String parameterName = bindableParameter.getName().orElseThrow(() -> new IllegalStateException(PARAMETER_NEEDS_TO_BE_NAMED));
			
			Object value = accessor.getBindableValue(bindableParameter.getIndex());
			
			Class<?> valueType;
			if (value instanceof Iterable) {
				ResolvableType resolvableType = bindableParameter.getResolvableType();
				valueType = resolvableType.getGeneric(0).resolve();
			} else if (value.getClass().isArray()) {
				valueType = value.getClass().getComponentType();
			} else {
				valueType = value.getClass();
			}
			PreparedStatementWriter<?> writer = dialect.getColumnBinderRegistry().getWriter(valueType);
			result.put(parameterName, writer);
		}
		
		return result;
	}
}
