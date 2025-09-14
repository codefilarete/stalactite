package org.codefilarete.stalactite.spring.repository.query.nativ;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.spring.repository.query.AbstractQueryExecutor;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethod;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethodInvocationParameters;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.result.ColumnedRowIterator;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.StringParamedSQL;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;

import static org.codefilarete.stalactite.spring.repository.query.projection.PartTreeStalactiteProjection.buildHierarchicMap;

/**
 * Classes for cases where the SQL given by @{@link org.codefilarete.stalactite.spring.repository.query.NativeQuery} annotation is expected to give
 * a projection of domain entities. The result is expected to be a list of maps, one map per row to suit Spring Data way of creating projections.
 *
 * @author Guillaume Mary
 */
public class TupleNativeQueryExecutor extends AbstractQueryExecutor<List<Map<String, Object>>, Map<String, Object>> {
	
	private final IdentityHashMap<JoinLink<?, ?>, String> expectedAliasesInNativeQuery;
	private final IdentityHashMap<JoinLink<?, ?>, AccessorChain<?, ?>> columnToProperties;
	private final Supplier<Limit> limitSupplier;
	private final String sql;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	
	public TupleNativeQueryExecutor(StalactiteQueryMethod method,
									String sql,
									Dialect dialect,
									ConnectionProvider connectionProvider,
									IdentityHashMap<? extends JoinLink<?, ?>, String> expectedAliasesInNativeQuery,
									IdentityHashMap<? extends JoinLink<?, ?>, ? extends AccessorChain<?, ?>> columnToProperties,
									Supplier<Limit> limitSupplier) {
		super(method);
		this.dialect = dialect;
		this.sql = sql;
		this.connectionProvider = connectionProvider;
		this.expectedAliasesInNativeQuery = (IdentityHashMap<JoinLink<?, ?>, String>) expectedAliasesInNativeQuery;
		this.columnToProperties = (IdentityHashMap<JoinLink<?, ?>, AccessorChain<?, ?>>) columnToProperties;
		this.limitSupplier = limitSupplier;
	}
	
	@Override
	public Supplier<List<Map<String, Object>>> buildQueryExecutor(StalactiteQueryMethodInvocationParameters invocationParameters) {
		return () -> {
			Map<String, Object> values = invocationParameters.getNamedValues();
			
			Map<String, PreparedStatementWriter<?>> parameterBinders = invocationParameters.bindParameters(dialect);
			String sqlToExecute = sql;
			// Taking pageable parameter into account: at first glance we could have thought that asking the user to add some "limit" and "offset"
			// clauses to its SQL was enough, but it's not due to the offset clause that is actually optional. Indeed, the offset is only required
			// for "next" pages, only the very first doesn't 
			if (invocationParameters.getParameters().hasPageableParameter()) {
				Limit limit = limitSupplier.get();
				if (limit != null) {
					sqlToExecute += " limit :limit";
					values.put("limit", limit.getCount());
					parameterBinders.put("limit", DefaultParameterBinders.INTEGER_BINDER);
					if (limit.getOffset() != null) {
						sqlToExecute += " offset :offset";
						values.put("offset", limit.getOffset());
						parameterBinders.put("offset", DefaultParameterBinders.INTEGER_BINDER);
					}
				}
			}
			
			StringParamedSQL statement = new StringParamedSQL(sqlToExecute, parameterBinders);
			statement.setValues(values);
			
			try (ReadOperation<String> readOperation = dialect.getReadOperationFactory().createInstance(statement, connectionProvider)) {
//						readOperation.setListener((SQLOperation.SQLOperationListener<ParamType>) operationListener);
				// Note that setValues must be done after operationListener set
				readOperation.setValues(statement.getValues());
				ResultSet resultSet = readOperation.execute();
				// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
				Map<Selectable<?>, ResultSetReader<?>> columnReaders = new HashMap<>();
				expectedAliasesInNativeQuery.forEach((selectable, alias) -> {
					if (selectable instanceof Column) {
						columnReaders.put(selectable, dialect.getColumnBinderRegistry().getBinder((Column) selectable));
					} else {
						columnReaders.put(selectable, dialect.getColumnBinderRegistry().getBinder(selectable.getJavaType()));
					}
				});
				ColumnedRowIterator rowIterator = new ColumnedRowIterator(resultSet, columnReaders, expectedAliasesInNativeQuery);
				
				Accumulator<ColumnedRow, List<Map<String, Object>>, List<Map<String, Object>>> accumulator = new Accumulator<ColumnedRow, List<Map<String, Object>>, List<Map<String, Object>>>() {
					@Override
					public Supplier<List<Map<String, Object>>> supplier() {
						return LinkedList::new;
					}
					
					@Override
					public BiConsumer<List<Map<String, Object>>, ColumnedRow> aggregator() {
						return (finalResult, databaseRowDataProvider) -> {
							Map<String, Object> row = new HashMap<>();
							finalResult.add(row);
							for (Entry<JoinLink<?, ?>, AccessorChain<?, ?>> entry : columnToProperties.entrySet()) {
								buildHierarchicMap(entry.getValue(), databaseRowDataProvider.get(entry.getKey()), row);
							}
						};
					}
					
					@Override
					public Function<List<Map<String, Object>>, List<Map<String, Object>>> finisher() {
						return Function.identity();
					}
				};
				return accumulator.collect(() -> rowIterator);
			} catch (RuntimeException e) {
				throw new SQLExecutionException(statement.getSQL(), e);
			}
		};
	}
}