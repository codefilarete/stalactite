package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.query.api.QualifiedSelectable;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.builder.ExpandableSQLAppender;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory;
import org.codefilarete.stalactite.query.model.Placeholder;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.operator.In;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.QuerySQLBuilderFactoryBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.result.ColumnedRowIterator;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.CompositeTypeBinder;
import org.codefilarete.stalactite.sql.statement.binder.DelegatingCompositeTypeBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Maps;

import static org.codefilarete.tool.collection.Iterables.first;

/**
 * Executor of a select-by-source-identifiers query, shared by every separate-loading fetcher that needs to select
 * rows of a "right" table (a map entries table, an association table, ...) from a chunk of "left" (source) entity
 * identifiers : {@link org.codefilarete.stalactite.engine.configurer.resolver.map.MapEntryLoader} and
 * {@link AssociationTableLoader} for instance.
 * <p>
 * It builds the select query once (out of a given {@link EntityTreeQuery}) and reuses it for every chunk of
 * identifiers given to {@link #select(List)}, relying on a {@link Placeholder} bound to a dedicated
 * {@link ParameterBinder} that knows how to widespread a list of values (simple or composed of several columns) onto
 * the {@link java.sql.PreparedStatement}. This avoids building a new query (and its SQL string) for each chunk.
 *
 * @param <ROW> type of the rows returned by the select
 * @param <SRCID> identifier type of the source ("left") entities the query is filtered on
 * @param <LEFTTABLE> table of the source entities
 * @param <TARGETTABLE> table onto which the select is done (the "right" table)
 * @author Guillaume Mary
 */
public class SeparateFetchQueryExecutor<ROW, SRCID, LEFTTABLE extends Table<LEFTTABLE>, TARGETTABLE extends Table<TARGETTABLE>> {
	
	private final Query query;
	private final EntityTreeInflater<ROW> inflater;
	private final Map<Selectable<?>, ResultSetReader<?>> selectParameterBinders;
	private final Map<Selectable<?>, String> columnAliases;
	private final IdMapping<?, SRCID> sourceIdMapping;
	private final Map<? extends QualifiedSelectable<LEFTTABLE, ?>, ? extends QualifiedSelectable<TARGETTABLE, ?>> reverseForeignKey;
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	private final QuerySQLBuilderFactory querySQLBuilderFactory;
	
	public SeparateFetchQueryExecutor(EntityTreeQuery<ROW> entityTreeQuery,
	                                  IdMapping<?, SRCID> sourceIdMapping,
	                                  Map<? extends QualifiedSelectable<LEFTTABLE, ?>, ? extends QualifiedSelectable<TARGETTABLE, ?>> reverseForeignKey,
	                                  Dialect dialect,
	                                  ConnectionProvider connectionProvider) {
		this(entityTreeQuery.getQuery(), entityTreeQuery.getInflater(), entityTreeQuery.getSelectParameterBinders(), entityTreeQuery.getColumnAliases(),
				sourceIdMapping, reverseForeignKey, dialect, connectionProvider);
	}
	
	public SeparateFetchQueryExecutor(Query query,
	                                  EntityTreeInflater<ROW> inflater,
	                                  Map<Selectable<?>, ? extends ResultSetReader<?>> selectParameterBinders,
	                                  Map<Selectable<?>, String> columnAliases,
	                                  IdMapping<?, SRCID> sourceIdMapping,
	                                  Map<? extends QualifiedSelectable<LEFTTABLE, ?>, ? extends QualifiedSelectable<TARGETTABLE, ?>> reverseForeignKey,
	                                  Dialect dialect,
	                                  ConnectionProvider connectionProvider) {
		this.query = query;
		this.inflater = inflater;
		this.selectParameterBinders = (Map<Selectable<?>, ResultSetReader<?>>) selectParameterBinders;
		this.columnAliases = columnAliases;
		this.sourceIdMapping = sourceIdMapping;
		this.reverseForeignKey = reverseForeignKey;
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
		
		// We create a local registry to avoid polluting dialect's one
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry(dialect.getColumnBinderRegistry());
		this.querySQLBuilderFactory = new QuerySQLBuilderFactoryBuilder(
				dialect.getDmlNameProviderFactory(),
				columnBinderRegistry,
				dialect.getSqlTypeRegistry().getJavaTypeToSqlTypeMapping())
				.build();
		
		if (sourceIdMapping.getIdentifierAssembler() instanceof ComposedIdentifierAssembler) {
			Map<Column<LEFTTABLE, ?>, Column<TARGETTABLE, ?>> typedReverseForeignKey = (Map) reverseForeignKey;
			Map<Column<TARGETTABLE, ?>, Integer> columnIndexes = new HashMap<>();
			Map<Integer, PreparedStatementWriter<?>> psWriters = new HashMap<>();
			int i = 0;
			for (Column<TARGETTABLE, ?> targetTableKeyColumn : typedReverseForeignKey.values()) {
				columnIndexes.put(targetTableKeyColumn, i);
				psWriters.put(i, columnBinderRegistry.getBinder(targetTableKeyColumn));
				i++;
			}
			
			DelegatingCompositeTypeBinder<SRCID> compositeTypeBinder = new DelegatingCompositeTypeBinder<>(
					sourceIdMapping.getIdentifierType(),
					psWriters,
					(SRCID srcid) -> {
						Map<Column<LEFTTABLE, ?>, ?> identifierValues = ((ComposedIdentifierAssembler<SRCID, LEFTTABLE>) sourceIdMapping.getIdentifierAssembler()).getColumnValues(srcid);
						Map<Column<TARGETTABLE, ?>, ?> columnValues = Maps.innerJoin(typedReverseForeignKey, identifierValues);
						
						Object[] objects = new Object[columnIndexes.size()];
						columnValues.forEach((column, columnValue) -> objects[columnIndexes.get(column)] = columnValue);
						return objects;
					}
			);
			
			SmartListCompositeParameterBinder<SRCID> smartListBinder = new SmartListCompositeParameterBinder<>(compositeTypeBinder);
			columnBinderRegistry.register(sourceIdMapping.getIdentifierType(), (ParameterBinder<SRCID>) smartListBinder);
		} else {
			// We take the exact primary column type binder, not the sourceIdMapping.getIdentifierType(), because it could be
			// specifically defined by the user
			Column<LEFTTABLE, SRCID> pkColumn = (Column<LEFTTABLE, SRCID>) first(sourceIdMapping.getIdentifierAssembler().getColumns());
			SmartListParameterBinder<SRCID> smartListBinder = new SmartListParameterBinder<>(columnBinderRegistry.getBinder(pkColumn));
			columnBinderRegistry.register(sourceIdMapping.getIdentifierType(), (ParameterBinder<SRCID>) smartListBinder);
		}
	}
	
	public Set<ROW> select(List<SRCID> srcIds) {
		String idsParameterName = "ids";
		if (sourceIdMapping.getIdentifierAssembler() instanceof ComposedIdentifierAssembler) {
			if (!dialect.supportsTupleCondition()) {
				throw new UnsupportedOperationException("Tuple condition is not supported by the database dialect but composite identifier requires it for 2-phases loading :"
						+ Reflections.toString(sourceIdMapping.getIdentifierType()));
			}
			Column<TARGETTABLE, ?>[] array = reverseForeignKey.values().<Column<TARGETTABLE, ?>>toArray(new Column[0]);
			Placeholder<SRCID, List<Object[]>> placeholder = new Placeholder<>(idsParameterName, sourceIdMapping.getIdentifierType());
			TupleIn in = new TupleIn(array, placeholder);
			query.getWhere().and(in);
		} else {
			Column<TARGETTABLE, ?> pkColumn = (Column<TARGETTABLE, ?>) first(reverseForeignKey.values());
			Placeholder<SRCID, List<SRCID>> placeholder = new Placeholder<>(idsParameterName, sourceIdMapping.getIdentifierType());
			In<SRCID> in = new In<>(placeholder);
			query.getWhere().and(pkColumn, in);
		}
		
		QuerySQLBuilderFactory.QuerySQLBuilder sqlQueryBuilder = querySQLBuilderFactory.queryBuilder(query);
		ExpandableSQLAppender preparableSQL = sqlQueryBuilder.toPreparableSQL();
		
		return execute(preparableSQL.toPreparedSQL(Maps.asMap(idsParameterName, srcIds)));
	}
	
	private <ParamType> Set<ROW> execute(SQLStatement<ParamType> query) {
		try (ReadOperation<ParamType> readOperation = dialect.getReadOperationFactory().createInstance(query, connectionProvider)) {
			// Note that setValues must be done after operationListener set
			readOperation.setValues(query.getValues());
			return transform(readOperation);
		} catch (RuntimeException e) {
			throw new SQLExecutionException(query.getSQL(), e);
		}
	}
	
	private Set<ROW> transform(ReadOperation<?> closeableOperation) {
		ResultSet resultSet = closeableOperation.execute();
		// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
		ColumnedRowIterator rowIterator = new ColumnedRowIterator(resultSet, selectParameterBinders, columnAliases);
		return transform(rowIterator);
	}
	
	private Set<ROW> transform(Iterator<? extends ColumnedRow> rowIterator) {
		return inflater.transform(() -> (Iterator<ColumnedRow>) rowIterator, 50);
	}
	
	/**
	 * {@link ParameterBinder} that widespreads an {@link Iterable} value over several calls to {@link PreparedStatement#setObject(int, Object)}
	 * (through its delegate binder), one per element, all targeting the same parameter index. This is meant to be
	 * registered in place of the binder of an identifier type so it can be used to set a variable-size list of values as
	 * a single named parameter of a {@link org.codefilarete.stalactite.query.model.Placeholder}, which is expanded at
	 * {@link org.codefilarete.stalactite.sql.statement.ExpandableSQL} time.
	 * <p>
	 * Made to handle a single (non-composite) value that goes on a single column.
	 *
	 * @param <C> the type of the single value handled by this binder
	 * @author Guillaume Mary
	 * @see SmartListCompositeParameterBinder
	 */
	private static class SmartListParameterBinder<C> implements ParameterBinder<Object> {
		
		protected final ParameterBinder<C> singleValueBinder;
		
		private SmartListParameterBinder(ParameterBinder<C> singleValueBinder) {
			this.singleValueBinder = singleValueBinder;
		}
		
		@Override
		public void set(PreparedStatement preparedStatement, int valueIndex, Object value) throws SQLException {
			if (value instanceof Iterable) {
				((Iterable<?>) value).forEach(v -> {
					try {
						singleValueBinder.set(preparedStatement, valueIndex, (C) v);
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
				});
			} else {
				singleValueBinder.set(preparedStatement, valueIndex, (C) value);
			}
		}
		
		@Override
		public Object doGet(ResultSet resultSet, String columnName) throws SQLException {
			// we can't handle the read operation because it goes against this class principle : widespread a composite object over several columns
			throw new UnsupportedOperationException("This invocation is unexpected : this class was made to handle complex type set in a PreparedStatement, not to read them from a ResultSet");
		}
		
		@Override
		public Class<Object> getType() {
			return (Class<Object>) singleValueBinder.getType();
		}
	}
	
	/**
	 * {@link SmartListParameterBinder} variant for composite (multi-column) identifier types : delegates to a
	 * {@link CompositeTypeBinder} instead of a plain {@link org.codefilarete.stalactite.sql.statement.binder.ParameterBinder}
	 * so it also exposes the number of columns the composite type spans over.
	 *
	 * @param <C> the composite type handled by this binder
	 * @author Guillaume Mary
	 */
	private static class SmartListCompositeParameterBinder<C> extends SmartListParameterBinder<C> implements CompositeTypeBinder<Object> {
		
		private SmartListCompositeParameterBinder(CompositeTypeBinder<C> componentTypeBinders) {
			super(componentTypeBinders);
		}
		
		@Override
		public int getComponentTypeSize() {
			return ((CompositeTypeBinder<C>) singleValueBinder).getComponentTypeSize();
		}
	}
}
