package org.codefilarete.stalactite.spring.repository.query.projection;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.spring.repository.query.PartTreeStalactiteQuery;
import org.codefilarete.stalactite.spring.repository.query.StalactiteLimitRepositoryQuery;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * Kind of {@link PartTreeStalactiteQuery} made for partial result query like {@link Slice} and {@link Page} ones.
 * Designed as a wrapper of {@link PartTreeStalactiteQuery} because its behavior is just a tweak of the real query.
 * 
 * @param <C>
 * @param <R>
 * @author Guillaume Mary
 */
public class PartTreeStalactiteLimitingQuery<C, R> implements RepositoryQuery {
	
	private final StalactiteLimitRepositoryQuery<C, R> delegate;
	
	public PartTreeStalactiteLimitingQuery(QueryMethod method,
										   AdvancedEntityPersister<C, ?> entityPersister,
										   PartTree tree,
										   StalactiteLimitRepositoryQuery<C, R> delegate) {
		this.delegate = delegate;
	}
	
	@Override
	public R execute(Object[] parameters) {
		StalactiteParametersParameterAccessor smartParameters = new StalactiteParametersParameterAccessor(getQueryMethod().getParameters(), parameters);
		Pageable pageable = smartParameters.getPageable();
		if (delegate.getQueryMethod().isSliceQuery() && pageable.getPageNumber() == 0) {
			// The + 1 is a look-ahead tip to make the returned Slice eventually return true on hasNext()
			delegate.limit(pageable.getPageSize() + 1);
		} else {
			// when the user asks for a page number (given Pageable is a Page instance or a Slice with page number) then we ask for the page number
			delegate.limit(pageable.getPageSize(), (int) pageable.getOffset());
		}
		return delegate.execute(parameters);
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
