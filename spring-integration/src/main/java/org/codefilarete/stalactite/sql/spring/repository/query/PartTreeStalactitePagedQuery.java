package org.codefilarete.stalactite.sql.spring.repository.query;

import java.util.List;
import java.util.function.BiFunction;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.support.PageableExecutionUtils;

/**
 * Kind of {@link PartTreeStalactiteQuery} made for partial result query like {@link Slice} and {@link Page} ones.
 * Designed as a wrapper of {@link PartTreeStalactiteQuery} because its behavior is just a tweak of the real query.
 * 
 * @param <C>
 * @param <R>
 * @author Guillaume Mary
 */
public class PartTreeStalactitePagedQuery<C, R extends Slice<C>> implements RepositoryQuery {
	
	private final PartTreeStalactiteQuery<C, List<C>> delegate;
	private final BiFunction<StalactiteParametersParameterAccessor, List<C>, R> queryResultSlicer;
	
	public PartTreeStalactitePagedQuery(QueryMethod method,
										AdvancedEntityPersister<C, ?> entityPersister,
										PartTree tree) {
		this.delegate = new PartTreeStalactiteQuery<>(method, entityPersister, tree, Accumulators.toList());
		if (method.isSliceQuery()) {
			this.queryResultSlicer = (accessor, result) -> {
				int pageSize = 0;
				Pageable pageable = accessor.getPageable();
				if (pageable.isPaged()) {
					pageSize = pageable.getPageSize();
				}
				boolean hasNext = pageable.isPaged() && result.size() > pageSize;
				return (R) new SliceImpl<>(hasNext ? result.subList(0, pageSize) : result, pageable, hasNext);
			};
		} else if (method.isPageQuery()) {
			PartTreeStalactiteProjection<C, Long> countQuery = PartTreeStalactiteProjection.forCount(method, entityPersister, tree);
			this.queryResultSlicer = (accessor, result) -> (R) PageableExecutionUtils.getPage(result, accessor.getPageable(), () -> countQuery.execute(accessor.getValues()));
		} else {
			throw new IllegalArgumentException("Query is not pageable " + method);
		}
	}
	
	@Override
	public R execute(Object[] parameters) {
		StalactiteParametersParameterAccessor smartParameters = new StalactiteParametersParameterAccessor(getQueryMethod().getParameters(), parameters);
		Pageable pageable = smartParameters.getPageable();
		if (delegate.getQueryMethod().isSliceQuery() && pageable.getPageNumber() == 0) {
			// The + 1 is a look-ahead tip to make the returned Slice eventually return true on hasNext()
			delegate.query.executableEntityQuery.getQueryPageSupport().limit(pageable.getPageSize() + 1);
		} else {
			// when the user asks for a page number (given Pageable is a Page instance or a Slice with page number) then we ask for the page number
			delegate.query.executableEntityQuery.getQueryPageSupport().limit(pageable.getPageSize(), (int) pageable.getOffset());
		}
		return queryResultSlicer.apply(smartParameters, delegate.execute(parameters));
	}
	
	@Override
	public QueryMethod getQueryMethod() {
		return delegate.getQueryMethod();
	}
	
	public static class StalactiteParametersParameterAccessor extends ParametersParameterAccessor {
		
		/**
		 * Constructor matching super one.
		 */
		public StalactiteParametersParameterAccessor(Parameters<?, ?> parameters, Object[] values) {
			super(parameters, values);
		}
		
		/**
		 * Overridden to make it public 
		 * @return current method parameters values
		 */
		@Override
		public Object[] getValues() {
			return super.getValues();
		}
	}
}
