package org.codefilarete.stalactite.spring.repository.query;

import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultCollectioner;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultPager;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultReducer;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultSingler;
import org.codefilarete.stalactite.spring.repository.query.reduce.QueryResultSlicer;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.tool.Reflections;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;

/**
 * Parent class of some {@link RepositoryQuery} to share some code
 * @author Guillaume Mary
 */
public abstract class AbstractRepositoryQuery<C, R> implements StalactiteRepositoryQuery<C, R> {

	protected final StalactiteQueryMethod method;
	
	public AbstractRepositoryQuery(StalactiteQueryMethod method) {
		this.method = method;
	}

	@Override
	public StalactiteQueryMethod getQueryMethod() {
		return method;
	}
	
	@Override
	public R execute(Object[] parameters) {
		StalactiteQueryMethodInvocationParameters accessor = new StalactiteQueryMethodInvocationParameters(method, parameters);
		AbstractQueryExecutor<List<Object>, Object> queryExecutor = buildQueryExecutor(accessor);
		Supplier<List<Object>> resultSupplier = queryExecutor.buildQueryExecutor(parameters);
		
		R adaptation = buildResultReducer(accessor)
				.adapt(resultSupplier)
				.apply(parameters);
		
		ResultProcessor resultProcessor = buildResultProcessor(parameters);
		return resultProcessor.processResult(adaptation);
	}
	
	protected abstract AbstractQueryExecutor<List<Object>, Object> buildQueryExecutor(StalactiteQueryMethodInvocationParameters invocationParameters);
	
	protected <ROW> QueryResultReducer<R, ROW> buildResultReducer(StalactiteQueryMethodInvocationParameters invocationParameters) {
		QueryResultReducer<?, C> result;
		switch (method.getQueryMethodReturnType()) {
			case COLLECTION:
				result = new QueryResultCollectioner<>();
				break;
			case PAGE:
				result = new QueryResultPager<>(this, invocationParameters, buildCountSupplier(invocationParameters));
				break;
			case SLICE:
				result = new QueryResultSlicer<>(this, invocationParameters);
				break;
//			case STREAM:
//				result = new QueryResultStreamer<>(this);
//				break;
			case SINGLE_ENTITY:
				result = new QueryResultSingler<>();
				break;
			case SINGLE_PROJECTION:
				result = new QueryResultSingler<>();
				break;
			default:
				throw new IllegalArgumentException("Unsupported return type for method " + Reflections.toString(method.getMethod()));
		}
		return (QueryResultReducer<R, ROW>) result;
	}
	
	protected abstract LongSupplier buildCountSupplier(StalactiteQueryMethodInvocationParameters accessor);
	
	
	protected ResultProcessor buildResultProcessor(Object[] parameters) {
		// - hasDynamicProjection() is for case of method that gives the expected returned type as a last argument (or a compound one by Collection or other)
		ResultProcessor resultProcessor = method.getResultProcessor();
		if (method.getParameters().hasDynamicProjection()) {
			resultProcessor = resultProcessor.withDynamicProjection(new ParametersParameterAccessor(method.getParameters(), parameters));
		}
		return resultProcessor;
	}
	
}