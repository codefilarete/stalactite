package org.codefilarete.stalactite.spring.repository.query;

import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.codefilarete.stalactite.query.model.Limit;
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
public abstract class AbstractRepositoryQuery<C, R> implements StalactiteLimitRepositoryQuery<C, R> {

	protected final StalactiteQueryMethod method;
	private Limit limit;
	
	public AbstractRepositoryQuery(StalactiteQueryMethod method) {
		this.method = method;
	}

	@Override
	public StalactiteQueryMethod getQueryMethod() {
		return method;
	}
	
	public Limit getLimit() {
		return limit;
	}
	
	@Override
	public void limit(int count) {
		limit = new Limit(count);
	}
	
	@Override
	public void limit(int count, Integer offset) {
		limit = new Limit(count, offset);
	}
	
	@Override
	public R execute(Object[] parameters) {
		StalactiteParametersParameterAccessor accessor = new StalactiteParametersParameterAccessor(method.getParameters(), parameters);
		AbstractQueryExecutor<List<Object>, Object> queryExecutor = buildQueryExecutor(accessor);
		Supplier<List<Object>> resultSupplier = queryExecutor.buildQueryExecutor(parameters);
		
		R adaptation = buildResultReducer(accessor, queryExecutor.bindParameters(accessor))
				.adapt(resultSupplier)
				.apply(parameters);
		
		ResultProcessor resultProcessor = buildResultProcessor(parameters);
		return resultProcessor.processResult(adaptation);
	}
	
	protected abstract AbstractQueryExecutor<List<Object>, Object> buildQueryExecutor(StalactiteParametersParameterAccessor accessor);
	
	protected <ROW> QueryResultReducer<R, ROW> buildResultReducer(StalactiteParametersParameterAccessor accessor, Map<String, PreparedStatementWriter<?>> bindParameters) {
		QueryResultReducer<?, C> result;
		switch (method.getQueryMethodReturnType()) {
			case COLLECTION:
				result = new QueryResultCollectioner<>();
				break;
			case PAGE:
				result = new QueryResultPager<>(this, buildCountSupplier(accessor, bindParameters));
				break;
			case SLICE:
				result = new QueryResultSlicer<>(this);
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
	
	protected abstract LongSupplier buildCountSupplier(StalactiteParametersParameterAccessor accessor, Map<String, PreparedStatementWriter<?>> bindParameters);
	
	
	protected ResultProcessor buildResultProcessor(Object[] parameters) {
		// - hasDynamicProjection() is for case of method that gives the expected returned type as a last argument (or a compound one by Collection or other)
		ResultProcessor resultProcessor = method.getResultProcessor();
		if (method.getParameters().hasDynamicProjection()) {
			resultProcessor = resultProcessor.withDynamicProjection(new ParametersParameterAccessor(method.getParameters(), parameters));
		}
		return resultProcessor;
	}
	
}