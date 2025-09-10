package org.codefilarete.stalactite.spring.repository.query;

import java.lang.reflect.Method;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.sql.Dialect;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * {@link QueryLookupStrategy} that creates a query for each candidate derived-query-method.
 * 
 * @param <T>
 * @author Guillaume Mary
 */
public class CreateQueryLookupStrategy<T> implements QueryLookupStrategy {
	
	private final AdvancedEntityPersister<T, ?> entityPersister;
	private final Dialect dialect;
	
	public CreateQueryLookupStrategy(AdvancedEntityPersister<T, ?> entityPersister,
									 Dialect dialect) {
		this.entityPersister = entityPersister;
		this.dialect = dialect;
	}
	
	@Override
	public RepositoryQuery resolveQuery(Method method,
										RepositoryMetadata metadata,
										ProjectionFactory factory,
										NamedQueries namedQueries) {
		PartTree partTree = new PartTree(method.getName(), entityPersister.getClassToPersist());
		StalactiteQueryMethod queryMethod = new StalactiteQueryMethod(method, metadata, factory);
		
		new QueryMethodValidator(partTree, queryMethod).validate();
		if (partTree.isDelete()) {
			return new PartTreeStalactiteDelete<>(queryMethod, entityPersister, partTree, dialect);
		} else if (partTree.isCountProjection()) {
			return new PartTreeStalactiteCountProjection<>(queryMethod, entityPersister, partTree);
		} else if (partTree.isExistsProjection()) {
			return new PartTreeStalactiteExistsProjection<>(queryMethod, entityPersister, partTree);
		} else if ((queryMethod.getResultProcessor().getReturnedType().isProjecting()
				&& factory.getProjectionInformation(queryMethod.getReturnedObjectType()).isClosed())
				|| queryMethod.getParameters().hasDynamicProjection()
		) {
			// The projection is closed: it means there's not @Value on the interface, so we can use Spring property introspector to look up for
			// properties to select in the query
			// If the projection is open (any method as a @Value on it), then, because Spring can't know in advance which field will be required to
			// evaluate the @Value expression, we must retrieve the whole aggregate as entities.
			// se https://docs.spring.io/spring-data/jpa/reference/repositories/projections.html
			return new PartTreeStalactiteProjection<>(queryMethod, entityPersister, partTree, factory, dialect);
		} else {
			return new PartTreeStalactiteQuery<>(queryMethod, entityPersister, partTree, dialect);
		}
	}
}
