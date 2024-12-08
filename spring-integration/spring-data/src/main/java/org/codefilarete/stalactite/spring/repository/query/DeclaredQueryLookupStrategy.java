package org.codefilarete.stalactite.spring.repository.query;

import java.lang.reflect.Method;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.spring.repository.query.nativ.NativeQueryMethod;
import org.codefilarete.stalactite.spring.repository.query.nativ.SqlNativeRepositoryQuery;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.tool.Nullable;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;

/**
 * {@link QueryLookupStrategy} that tries to detect a declared query declared via {@link Query} annotation.
 *
 * @author Guillaume Mary
 */
public class DeclaredQueryLookupStrategy<C> implements QueryLookupStrategy {
	
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	private final AdvancedEntityPersister<C, ?> entityPersister;
	
	/**
	 * Creates a new {@link DeclaredQueryLookupStrategy}.
	 *
	 */
	public DeclaredQueryLookupStrategy(AdvancedEntityPersister<C, ?> entityPersister,
									   Dialect dialect,
									   ConnectionProvider connectionProvider) {
		this.entityPersister = entityPersister;
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
	}
	
	/**
	 * @return null if no declared query is found on the method through the {@link Query} annotation
	 */
	@Override
	public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory, NamedQueries namedQueries) {
		Nullable<String> sql = Nullable.nullable(method.getAnnotation(Query.class)).map(Query::value);
		if (sql.isPresent()) {
			NativeQueryMethod queryMethod = new NativeQueryMethod(method, metadata, factory);
			Accumulator<C, ?, ?> accumulator = queryMethod.isCollectionQuery()
					? (Accumulator) Accumulators.toKeepingOrderSet()
					: (Accumulator) Accumulators.getFirstUnique();
			return new SqlNativeRepositoryQuery<>(queryMethod, sql.get(), entityPersister, accumulator, dialect, connectionProvider);
		} else {
			return null;
		}
	}
}