package org.codefilarete.stalactite.spring.repository.query.nativ;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.spring.repository.query.NativeQueries;
import org.codefilarete.stalactite.spring.repository.query.NativeQuery;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethod;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.VisibleForTesting;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;

import static org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

/**
 * {@link QueryLookupStrategy} that tries to detect a query declared via {@link NativeQuery} annotation.
 *
 * @author Guillaume Mary
 */
public class NativeQueryLookupStrategy<C> implements QueryLookupStrategy {
	
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	private final AdvancedEntityPersister<C, ?> entityPersister;
	
	/**
	 * Creates a new {@link NativeQueryLookupStrategy}.
	 *
	 */
	public NativeQueryLookupStrategy(AdvancedEntityPersister<C, ?> entityPersister,
									 Dialect dialect,
									 ConnectionProvider connectionProvider) {
		this.entityPersister = entityPersister;
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
	}
	
	/**
	 * @return null if no declared query is found on the method through the {@link NativeQuery} annotation
	 */
	@Override
	public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory, NamedQueries namedQueries) {
		NativeQuery nativeQueryAnnotation = findSQL(method);
		if (nativeQueryAnnotation != null) {
			String sql = nativeQueryAnnotation.value();
			String sqlCount = nativeQueryAnnotation.countQuery();
			StalactiteQueryMethod nativeQueryMethod = new StalactiteQueryMethod(method, metadata, factory);
			
			return new SqlNativeRepositoryQuery<>(
					nativeQueryMethod,
					sql,
					sqlCount,
					entityPersister,
					factory,
					dialect,
					connectionProvider);
		} else {
			return null;
		}
	}

	@VisibleForTesting
	@javax.annotation.Nullable
	NativeQuery findSQL(Method method) {
		Nullable<List<NativeQuery>> queries = Nullable.nullable(method.getAnnotation(NativeQueries.class)).map(NativeQueries::value).map(Arrays::asList);
		if (queries.isPresent()) {
			// Several @NativeQuery found, we look up for the best that suits Dialect compatibility
			TreeMap<DatabaseSignet, NativeQuery> dialectPerSortedCompatibility = new TreeMap<>(DatabaseSignet.COMPARATOR);
			queries.get().forEach(query -> dialectPerSortedCompatibility.merge(new DatabaseSignet(query.vendor(), query.major(), query.minor()), query, (c1, c2) -> {
				// we use same properties as DatabaseSignet comparator ones since we use a TreeMap based on it 
				String printableSignet = Strings.footPrint(new DatabaseSignet(c1.vendor(), c1.major(), c1.minor()), DatabaseSignet::toString);
				throw new IllegalStateException("Multiple queries with same database compatibility found on method " + Reflections.toString(method) + " : " + printableSignet);
			}));

			DatabaseSignet currentSignet = dialect.getCompatibility();
			// we select the highest query among the smaller than database version
			Map.Entry<DatabaseSignet, NativeQuery> foundEntry = dialectPerSortedCompatibility.floorEntry(currentSignet);
			return Nullable.nullable(foundEntry).map(Map.Entry::getValue).get();
		} else {
			Nullable<NativeQuery> queryAnnotation = Nullable.nullable(method.getAnnotation(NativeQuery.class));
			if (queryAnnotation.isPresent()) {
				// we check a Dialect compatibility to let user override one particular database
				NativeQuery nativeQuery = queryAnnotation.get();
				int comparison = DatabaseSignet.COMPARATOR.compare(dialect.getCompatibility(), new DatabaseSignet(nativeQuery.vendor(), nativeQuery.major(), nativeQuery.minor()));
				if (comparison >= 0) {
					return nativeQuery;
				}
			}
		}
		return null;
	}
}