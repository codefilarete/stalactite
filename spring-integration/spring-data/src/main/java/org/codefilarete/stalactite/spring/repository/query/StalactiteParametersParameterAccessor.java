package org.codefilarete.stalactite.spring.repository.query;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.tool.collection.Arrays;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ParametersParameterAccessor;

/**
 * An enhanced version of {@link ParametersParameterAccessor} for Stalactite framework need.
 * 
 * @author Guillaume Mary
 */
public class StalactiteParametersParameterAccessor extends ParametersParameterAccessor {

	public static final String PARAMETER_NEEDS_TO_BE_NAMED = "For queries with named parameters you need to provide names for method parameters. Use @Param for query method parameters, or when on Java 8+ use the javac flag -parameters.";
	
	/**
	 * Constructor matching super one.
	 */
	public StalactiteParametersParameterAccessor(Parameters<?, ?> parameters, Object[] values) {
		super(parameters, values);
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
}
