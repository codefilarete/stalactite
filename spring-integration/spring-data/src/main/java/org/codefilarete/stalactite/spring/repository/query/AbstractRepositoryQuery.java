package org.codefilarete.stalactite.spring.repository.query;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.tool.collection.Arrays;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;

/**
 * Parent class of some {@link RepositoryQuery} to share some code
 * @author Guillaume Mary
 */
public abstract class AbstractRepositoryQuery implements RepositoryQuery {

	private static final String PARAMETER_NEEDS_TO_BE_NAMED = "For queries with named parameters you need to provide names for method parameters. Use @Param for query method parameters, or when on Java 8+ use the javac flag -parameters.";
	
	protected final QueryMethod queryMethod;

	public AbstractRepositoryQuery(QueryMethod queryMethod) {
		this.queryMethod = queryMethod;
	}

	@Override
	public QueryMethod getQueryMethod() {
		return queryMethod;
	}

	public Map<String, Object> getValues(ParametersParameterAccessor accessor) {
		Map<String, Object> result = new HashMap<>();
		for (Parameter bindableParameter : queryMethod.getParameters().getBindableParameters()) {
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
