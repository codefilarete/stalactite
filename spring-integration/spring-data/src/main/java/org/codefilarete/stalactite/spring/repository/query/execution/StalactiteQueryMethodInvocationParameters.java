package org.codefilarete.stalactite.spring.repository.query.execution;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethod;
import org.codefilarete.stalactite.spring.repository.query.execution.reduce.LimitHandler;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.tool.collection.Arrays;
import org.springframework.core.ResolvableType;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ParametersParameterAccessor;

/**
 * An enhanced version of {@link ParametersParameterAccessor} for Stalactite framework need.
 *
 * @author Guillaume Mary
 */
public class StalactiteQueryMethodInvocationParameters extends ParametersParameterAccessor implements LimitHandler {

	public static final String PARAMETER_NEEDS_TO_BE_NAMED = "For queries with named parameters you need to provide names for method parameters."
			+ " Use @Param for query method parameters, or when on Java 8+ use the javac flag -parameters.";
	
	private final StalactiteQueryMethod method;
	
	private Limit limit;
	
	/**
	 * Constructor matching super one.
	 */
	public StalactiteQueryMethodInvocationParameters(StalactiteQueryMethod method, Object[] values) {
		super(method.getParameters(), values);
		this.method = method;
	}

	/**
	 * Overridden to make it public
	 *
	 * @return current method parameters values
	 */
	@Override
	public Object[] getValues() {
		return super.getValues();
	}
	
	/**
	 * Returns the dynamic projection type ({@link Class} object in the values) if any, null otherwise.
	 * @return null if no dynamic projection is available
	 */
	public Class<?> getDynamicProjectionType() {
		if (getParameters().hasDynamicProjection()) {
			return (Class<?>) getValues()[getParameters().getDynamicProjectionIndex()];
		} else {
			return null;
		}
	}
	
	/**
	 * Transforms the values given at construction time into a {@link Map} of values according to their names found in given {@link Parameters}.
	 * @return a {@link Map} of values per their names
	 */
	public Map<String, Object> getNamedValues() {
		Map<String, Object> result = new HashMap<>();
		for (Parameter bindableParameter : getParameters().getBindableParameters()) {
			String parameterName = bindableParameter.getName().orElseThrow(() -> new IllegalStateException(PARAMETER_NEEDS_TO_BE_NAMED));
			Object value = getBindableValue(bindableParameter.getIndex());
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
	
	public Map<String, PreparedStatementWriter<?>> bindParameters(Dialect dialect) {
		Map<String, PreparedStatementWriter<?>> result = new HashMap<>();
		RelationalParameters bindableParameters = method.getParameters().getBindableParameters();
		
		for (RelationalParameters.RelationalParameter bindableParameter : bindableParameters) {
			String parameterName = bindableParameter.getName().orElseThrow(() -> new IllegalStateException(StalactiteQueryMethodInvocationParameters.PARAMETER_NEEDS_TO_BE_NAMED));
			
			Object value = getBindableValue(bindableParameter.getIndex());
			
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
	
	public void setLimit(Limit limit) {
		this.limit = limit;
	}
	
	public Limit getLimit() {
		return limit;
	}
	
	@Override
	public void limit(int count) {
		setLimit(new Limit(count));
	}
	
	@Override
	public void limit(int count, Integer offset) {
		setLimit(new Limit(count, offset));
	}
}
