package org.codefilarete.stalactite.spring.repository.query;

import java.lang.reflect.Method;
import java.util.Collection;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryMethod;
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
	
	public CreateQueryLookupStrategy(AdvancedEntityPersister<T, ?> entityPersister) {
		this.entityPersister = entityPersister;
	}
	
	@Override
	public RepositoryQuery resolveQuery(Method method,
										RepositoryMetadata metadata,
										ProjectionFactory factory,
										NamedQueries namedQueries) {
		PartTree partTree = new PartTree(method.getName(), entityPersister.getClassToPersist());
		QueryMethod queryMethod = new QueryMethod(method, metadata, factory);
		
		new QueryMethodValidator(partTree, queryMethod).validate();
		if (partTree.isDelete()) {
			return new PartTreeStalactiteDelete<>(queryMethod, entityPersister, partTree);
		} else if (partTree.isCountProjection()) {
			return PartTreeStalactiteProjection.forCount(queryMethod, entityPersister, partTree);
		} else if (partTree.isExistsProjection()) {
			return PartTreeStalactiteProjection.forExists(queryMethod, entityPersister, partTree);
		} else {
			Accumulator<T, ?, ?> accumulator = queryMethod.isCollectionQuery()
					? (Accumulator) Accumulators.toKeepingOrderSet()
					: (Accumulator) Accumulators.getFirstUnique();
			if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
				return new PartTreeStalactitePagedQuery<>(queryMethod, entityPersister, partTree);
			} else {
				return new PartTreeStalactiteQuery<>(queryMethod, entityPersister, partTree, (Accumulator<T, ? extends Collection<T>, ?>) accumulator);
			}
		}
	}
}
