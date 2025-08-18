package org.codefilarete.stalactite.spring.repository.query;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.springframework.data.domain.Slice;
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
			return new PartTreeStalactiteCountProjection<>(queryMethod, entityPersister, partTree);
		} else if (partTree.isExistsProjection()) {
			return new PartTreeStalactiteExistsProjection<>(queryMethod, entityPersister, partTree);
		} else if ((queryMethod.getResultProcessor().getReturnedType().isProjecting()
				&& factory.getProjectionInformation(queryMethod.getReturnedObjectType()).isClosed())
				// TODO: handle dynamic projection
				//|| queryMethod.getParameters().hasDynamicProjection()
		) {
			// The projection is closed: it means there's not @Value on the interface, so we can use Spring property introspector to look up for
			// properties to select in the query
			// If the projection is open (any method as a @Value on it), then, because Spring can't know in advance which field will be required to
			// evaluate the @Value expression, we must retrieve the whole aggregate as entities.
			// se https://docs.spring.io/spring-data/jpa/reference/repositories/projections.html
			return new PartTreeStalactiteProjection<>(queryMethod, entityPersister, partTree, factory);
		} else {
			if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
				return createPageableQuery(queryMethod, partTree);
			} else {
				Accumulator<T, ?, ?> accumulator = queryMethod.isCollectionQuery()
						? (Accumulator) Accumulators.toKeepingOrderSet()
						: (Accumulator) Accumulators.getFirstUnique();
				return new PartTreeStalactiteQuery<>(queryMethod, entityPersister, partTree, (Accumulator<T, ? extends Collection<T>, ?>) accumulator);
			}
		}
	}
	
	private <R extends Slice<P>, P> PartTreeStalactitePagedQuery<T, R, P> createPageableQuery(QueryMethod queryMethod, PartTree partTree) {
		Accumulator<T, Collection<T>, List<P>> accumulator1 = new Accumulator<T, Collection<T>, List<P>>() {
			@Override
			public Supplier<Collection<T>> supplier() {
				return ArrayList::new;
			}
			
			@Override
			public BiConsumer<Collection<T>, T> aggregator() {
				return Collection::add;
			}
			
			@Override
			public Function<Collection<T>, List<P>> finisher() {
				return cs -> (List<P>) cs;
			}
		};
		PartTreeStalactiteQuery<T, List<P>> tListPartTreeStalactiteQuery = new PartTreeStalactiteQuery<>(queryMethod, entityPersister, partTree, accumulator1);
		return new PartTreeStalactitePagedQuery<>(queryMethod, entityPersister, partTree,
				tListPartTreeStalactiteQuery);
	}
}
