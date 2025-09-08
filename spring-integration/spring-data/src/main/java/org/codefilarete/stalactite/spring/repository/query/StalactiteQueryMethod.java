package org.codefilarete.stalactite.spring.repository.query;

import java.lang.reflect.Method;

import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.util.Lazy;

public class StalactiteQueryMethod extends QueryMethod {
	
	/** Shadow of super field "method" because its accessor is package-private */
	private final Method method;
	
	private final Lazy<QueryMethodReturnType> queryMethodReturnType;
	
	public StalactiteQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory factory) {
		super(method, metadata, factory);
		this.method = method;
		this.queryMethodReturnType = Lazy.of(() -> {
			if (isCollectionQuery())
				return QueryMethodReturnType.COLLECTION;
			if (isPageQuery())
				return QueryMethodReturnType.PAGE;
			if (isSliceQuery())
				return QueryMethodReturnType.SLICE;
//			if (isStreamQuery())
//				return QueryMethodReturnType.STREAM;
			if (isQueryForEntity())
				return QueryMethodReturnType.SINGLE_ENTITY;
			// default case: we suppose the result is a projection
			return QueryMethodReturnType.SINGLE_PROJECTION;
		});
	}
	
	@Override
	protected RelationalParameters createParameters(Method method) {
		return new RelationalParameters(method);
	}
	
	@Override
	public RelationalParameters getParameters() {
		return (RelationalParameters) super.getParameters();
	}
	
	public QueryMethodReturnType getQueryMethodReturnType() {
		return queryMethodReturnType.get();
	}

	/**
	 * Implemented to make super method accessible outside of Spring Data package
	 * {@inheritDoc}
	 */
	@Override
	public Class<?> getDomainClass() {
		return super.getDomainClass();
	}

	/**
	 * Implemented to make super method accessible outside of Spring Data package
	 * @return the original JDK method underlying this instance
	 */
	public Method getMethod() {
		return method;
	}
	
	public enum QueryMethodReturnType {
		COLLECTION,
		PAGE,
		SLICE,
//		STREAM,
		SINGLE_ENTITY,
		SINGLE_PROJECTION
	}
}