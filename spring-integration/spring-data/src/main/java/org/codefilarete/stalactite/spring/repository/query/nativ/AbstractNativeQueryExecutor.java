package org.codefilarete.stalactite.spring.repository.query.nativ;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.codefilarete.stalactite.spring.repository.query.projection.StalactiteParametersParameterAccessor;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.springframework.core.ResolvableType;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.repository.query.ParametersParameterAccessor;

public abstract class AbstractNativeQueryExecutor<C extends Collection<R>, R> implements QueryExecutor<C, R> {

	protected final NativeQueryMethod method;
	protected final Dialect dialect;

	public AbstractNativeQueryExecutor(NativeQueryMethod method, Dialect dialect) {
		this.method = method;
		this.dialect = dialect;
	}

	protected Map<String, PreparedStatementWriter<?>> bindParameters(ParametersParameterAccessor accessor) {
		Map<String, PreparedStatementWriter<?>> result = new HashMap<>();
		RelationalParameters bindableParameters = method.getParameters().getBindableParameters();

		for (RelationalParameters.RelationalParameter bindableParameter : bindableParameters) {
			String parameterName = bindableParameter.getName().orElseThrow(() -> new IllegalStateException(StalactiteParametersParameterAccessor.PARAMETER_NEEDS_TO_BE_NAMED));

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
