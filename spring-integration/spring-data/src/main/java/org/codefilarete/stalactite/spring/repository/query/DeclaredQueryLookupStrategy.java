package org.codefilarete.stalactite.spring.repository.query;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.spring.repository.query.nativ.NativeQueryMethod;
import org.codefilarete.stalactite.spring.repository.query.nativ.SqlNativeRepositoryQuery;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.VisibleForTesting;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;

import static org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

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
		String sql = findSQL(method);
		if (sql != null) {
			NativeQueryMethod queryMethod = new NativeQueryMethod(method, metadata, factory);
			Accumulator<C, ?, ?> accumulator = queryMethod.isCollectionQuery()
					? (Accumulator) Accumulators.toKeepingOrderSet()
					: (Accumulator) Accumulators.getFirstUnique();
			return new SqlNativeRepositoryQuery<>(queryMethod, sql, entityPersister, accumulator, dialect, connectionProvider);
		} else {
			return null;
		}
	}

	@VisibleForTesting
	@javax.annotation.Nullable
	String findSQL(Method method) {
		Nullable<List<Query>> queries = Nullable.nullable(method.getAnnotation(Queries.class)).map(Queries::value).map(Arrays::asList);
		if (queries.isPresent()) {
			// Several @Query found, we lookup for the best that suits Dialect compatibility
			TreeMap<DatabaseSignet, Query> dialectPerSortedCompatibility = new TreeMap<>(DatabaseSignet.COMPARATOR);
			queries.get().forEach(query -> dialectPerSortedCompatibility.merge(new DatabaseSignet(query.vendor(), query.major(), query.minor()), query, (c1, c2) -> {
				// we use same properties as DatabaseSignet comparator ones since we use a TreeMap based on it 
				String printableSignet = Strings.footPrint(new DatabaseSignet(c1.vendor(), c1.major(), c1.minor()), DatabaseSignet::toString);
				throw new IllegalStateException("Multiple queries with same database compatibility found : " + printableSignet);
			}));

			DatabaseSignet currentSignet = dialect.getCompatibility();
			// we select the highest query among the smaller than database version
			Map.Entry<DatabaseSignet, Query> foundEntry = dialectPerSortedCompatibility.floorEntry(currentSignet);
			return Nullable.nullable(foundEntry).map(e -> e.getValue().value()).get();
		} else {
			Nullable<Query> queryAnnotation = Nullable.nullable(method.getAnnotation(Query.class));
			if (queryAnnotation.isPresent()) {
				// we check a Dialect compatibility to let user override one particular database
				Query query = queryAnnotation.get();
				int comparison = DatabaseSignet.COMPARATOR.compare(dialect.getCompatibility(), new DatabaseSignet(query.vendor(), query.major(), query.minor()));
				if (comparison >= 0) {
					return query.value();
				}
			}
		}
		return null;
	}
}