package org.codefilarete.stalactite.spring.repository.query;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.spring.repository.query.reduce.LimitHandler;
import org.codefilarete.tool.collection.Arrays;
import org.springframework.data.domain.Pageable;
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
	
//	private final StalactiteQueryMethod method;
	
	private Limit limit;
	
	/**
	 * Constructor matching super one.
	 */
	public StalactiteQueryMethodInvocationParameters(StalactiteQueryMethod method, Object[] values) {
		super(method.getParameters(), values);
//		this.method = method;
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
//
//	private Limit getLimit() {
//		Pageable pageable = getPageable();
//		if (method.isSliceQuery() && pageable.getPageNumber() == 0) {
//			// The + 1 is a look-ahead tip to make the returned Slice eventually return true on hasNext()
//			return new Limit(pageable.getPageSize() + 1);
//		} else {
//			// when the user asks for a page number (given Pageable is a Page instance or a Slice with page number) then we ask for the page number
//			return new Limit(pageable.getPageSize(), (int) pageable.getOffset());
//		}
//	}
}
