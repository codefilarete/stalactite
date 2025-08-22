package org.codefilarete.stalactite.spring.repository.query.nativ;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.spring.repository.query.NativeQueries;
import org.codefilarete.stalactite.spring.repository.query.NativeQuery;
import org.codefilarete.stalactite.spring.repository.query.PartTreeStalactiteProjection;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.result.ColumnedRowIterator;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.StringParamedSQL;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.VisibleForTesting;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

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
		String sql = findSQL(method);
		if (sql != null) {
			NativeQueryMethod nativeQueryMethod = new NativeQueryMethod(method, metadata, factory);
			Accumulator<C, ?, ?> accumulator = nativeQueryMethod.isCollectionQuery()
					? (Accumulator) Accumulators.toKeepingOrderSet()
					: (Accumulator) Accumulators.getFirstUnique();
			SqlNativeRepositoryQuery<C> nativeRepositoryQuery = new SqlNativeRepositoryQuery<>(nativeQueryMethod, sql, entityPersister, accumulator, dialect, connectionProvider);

			QueryMethod queryMethod = new QueryMethod(method, metadata, factory);

			PartTree partTree = new PartTree("getByName", entityPersister.getClassToPersist());
			// TODO: change null by the partTree to make the Page feature works, else will get a NPE due to necessary Count query which requires the partTree
			return new PartTreeStalactiteProjection<C, Object>(queryMethod, entityPersister, partTree, factory) {
				@Override
				protected Supplier<List<Map<String, Object>>> buildQueryExecutor(Object[] parameters) {
					ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
					Map<String, PreparedStatementWriter<?>> parameterBinders = nativeRepositoryQuery.bindParameters(accessor);
					StringParamedSQL statement = new StringParamedSQL(sql, parameterBinders);
					Map<String, Object> values = nativeRepositoryQuery.getValues(accessor);
					statement.setValues(values);


					try (ReadOperation<String> readOperation = dialect.getReadOperationFactory().createInstance(statement, connectionProvider)) {
//						readOperation.setListener((SQLOperation.SQLOperationListener<ParamType>) operationListener);
						// Note that setValues must be done after operationListener set
						readOperation.setValues(statement.getValues());
						ResultSet resultSet = readOperation.execute();
						// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
						Map<Selectable<?>, ResultSetReader<?>> columnReaders = new HashMap<>();
						aliases.forEach((selectable, alias) -> {
							if (selectable instanceof Column) {
								columnReaders.put(selectable, dialect.getColumnBinderRegistry().getBinder((Column) selectable));
							} else {
								columnReaders.put(selectable, dialect.getColumnBinderRegistry().getBinder(selectable.getJavaType()));
							}
						});
						ColumnedRowIterator rowIterator = new ColumnedRowIterator(resultSet, columnReaders, aliases);


						Accumulator<ColumnedRow, List<Map<String, Object>>, List<Map<String, Object>>> zz = new Accumulator<ColumnedRow, List<Map<String, Object>>, List<Map<String, Object>>>() {
							@Override
							public Supplier<List<Map<String, Object>>> supplier() {
								return LinkedList::new;
							}

							@Override
							public BiConsumer<List<Map<String, Object>>, ColumnedRow> aggregator() {
								return (finalResult, databaseRowDataProvider) -> {
									Map<String, Object> row = new HashMap<>();
									finalResult.add(row);
									for (Map.Entry<Selectable<?>, PropertyPath> entry : columnToProperties.entrySet()) {
										buildHierarchicMap(entry.getValue().toDotPath(), databaseRowDataProvider.get(entry.getKey()), row);
									}
//									for (Selectable<?> selectable : aliases.keySet()) {
//										buildHierarchicMap(columnToProperties.get(selectable).toDotPath(), databaseRowDataProvider.get((Selectable<Object>) selectable), row);
//									}
								};
							}

							@Override
							public Function<List<Map<String, Object>>, List<Map<String, Object>>> finisher() {
								return Function.identity();
							}
						};
						return () -> zz.collect(() -> rowIterator);
					} catch (RuntimeException e) {
						throw new SQLExecutionException(statement.getSQL(), e);
					}
				}
			};
		} else {
			return null;
		}
	}

	@VisibleForTesting
	@javax.annotation.Nullable
	String findSQL(Method method) {
		Nullable<List<NativeQuery>> queries = Nullable.nullable(method.getAnnotation(NativeQueries.class)).map(NativeQueries::value).map(Arrays::asList);
		if (queries.isPresent()) {
			// Several @NativeQuery found, we lookup for the best that suits Dialect compatibility
			TreeMap<DatabaseSignet, NativeQuery> dialectPerSortedCompatibility = new TreeMap<>(DatabaseSignet.COMPARATOR);
			queries.get().forEach(query -> dialectPerSortedCompatibility.merge(new DatabaseSignet(query.vendor(), query.major(), query.minor()), query, (c1, c2) -> {
				// we use same properties as DatabaseSignet comparator ones since we use a TreeMap based on it 
				String printableSignet = Strings.footPrint(new DatabaseSignet(c1.vendor(), c1.major(), c1.minor()), DatabaseSignet::toString);
				throw new IllegalStateException("Multiple queries with same database compatibility found on method " + Reflections.toString(method) + " : " + printableSignet);
			}));

			DatabaseSignet currentSignet = dialect.getCompatibility();
			// we select the highest query among the smaller than database version
			Map.Entry<DatabaseSignet, NativeQuery> foundEntry = dialectPerSortedCompatibility.floorEntry(currentSignet);
			return Nullable.nullable(foundEntry).map(e -> e.getValue().value()).get();
		} else {
			Nullable<NativeQuery> queryAnnotation = Nullable.nullable(method.getAnnotation(NativeQuery.class));
			if (queryAnnotation.isPresent()) {
				// we check a Dialect compatibility to let user override one particular database
				NativeQuery nativeQuery = queryAnnotation.get();
				int comparison = DatabaseSignet.COMPARATOR.compare(dialect.getCompatibility(), new DatabaseSignet(nativeQuery.vendor(), nativeQuery.major(), nativeQuery.minor()));
				if (comparison >= 0) {
					return nativeQuery.value();
				}
			}
		}
		return null;
	}
}