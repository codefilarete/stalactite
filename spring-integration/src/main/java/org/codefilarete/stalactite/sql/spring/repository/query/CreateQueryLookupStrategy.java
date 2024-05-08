package org.codefilarete.stalactite.sql.spring.repository.query;

import java.lang.reflect.Method;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;

/**
 * {@link QueryLookupStrategy} that creates a query for each candidate derived-query-method.
 * 
 * @param <T>
 * @author Guillaume Mary
 */
public class CreateQueryLookupStrategy<T> implements QueryLookupStrategy {
	
	private final EntityPersister<T, ?> entityPersister;
	
	public CreateQueryLookupStrategy(EntityPersister<T, ?> entityPersister) {
		this.entityPersister = entityPersister;
	}
	
	@Override
	public RepositoryQuery resolveQuery(Method method,
										RepositoryMetadata metadata,
										ProjectionFactory factory,
										NamedQueries namedQueries) {
		return new PartTreeStalactiteQuery<>(new QueryMethod(method, metadata, factory), entityPersister);
	}
}
