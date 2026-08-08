package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecord;
import org.codefilarete.stalactite.engine.configurer.map.RecordId;
import org.codefilarete.stalactite.engine.configurer.resolver.map.EntryMapResolver.KeyValueRecordPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.IdentityLinkedMap;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.builder.ExpandableSQLAppender;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory;
import org.codefilarete.stalactite.query.model.Placeholder;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.operator.In;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.result.ColumnedRowIterator;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.binder.CompositeTypeBinder;
import org.codefilarete.stalactite.sql.statement.binder.DelegatingCompositeTypeBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;

import static org.codefilarete.tool.collection.Iterables.first;

/**
 * Particular {@link SelectExecutor} that loads {@link KeyValueRecord} from a map table by the source identifier.
 * Made for the separate loading of a {@link Map} entry values (as {@link KeyValueRecord}) by the source identifier : we
 * could have done it through the {@link KeyValueRecordPersister}, but its select is made through {@link RecordId}
 * which we don't have on separate loading since we only have the left entity identifiers available, whereas
 * {@link RecordId} is made of the left entity identifier and the key value.
 * 
 * @param <SRC>
 * @param <SRCID>
 * @param <K>
 * @param <V>
 * @param <LEFTTABLE>
 * @param <MAPTABLE>
 * @author Guillaume Mary
 */
public class MapEntryLoader<SRC, SRCID, K, V, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>> implements SelectExecutor<KeyValueRecord<K, V, SRCID>, SRCID> {
	
	private final EntityJoinTree<KeyValueRecord<K, V, SRCID>, RecordId<K, SRCID>> entityJoinTree;
	private final IdMapping<SRC, SRCID> sourceIdMapping;
	private final Map<JoinLink<LEFTTABLE, ?>, JoinLink<MAPTABLE, ?>> reverseForeignKey;
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	
	public MapEntryLoader(IdMapping<SRC, SRCID> sourceIdMapping,
	                      KeyValueRecordPersister<K, V, SRCID, MAPTABLE> keyValueRecordPersister,
	                      Map<JoinLink<LEFTTABLE, ?>, JoinLink<MAPTABLE, ?>> reverseForeignKey,
	                      Dialect dialect,
	                      ConnectionProvider connectionProvider) {
		this.sourceIdMapping = sourceIdMapping;
		this.reverseForeignKey = reverseForeignKey;
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
		DefaultEntityMapping<KeyValueRecord<K, V, SRCID>, RecordId<K, SRCID>, MAPTABLE> mapping = new DefaultEntityMapping<>(
				(Class) KeyValueRecord.class,
				keyValueRecordPersister.getMainTable(),
				keyValueRecordPersister.getMapping().getPropertyToColumn(),
				keyValueRecordPersister.getMapping().getIdMapping());
		this.entityJoinTree = new EntityJoinTree<>(mapping);
	}
	
	public EntityJoinTree<KeyValueRecord<K, V, SRCID>, RecordId<K, SRCID>> getEntityJoinTree() {
		return entityJoinTree;
	}
	
	@Override
	public Set<KeyValueRecord<K, V, SRCID>> select(Iterable<SRCID> ids) {
		int estimatedResultSize = Iterables.size(ids);
		// we avoid relying on Entity equals/Hashcode by using a Map based on System.identityHashCode(..)
		Set<KeyValueRecord<K, V, SRCID>> result = Collections.newSetFromMap(new IdentityLinkedMap<>(estimatedResultSize));
		EntityTreeQuery<KeyValueRecord<K, V, SRCID>> entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		InternalExecutor<KeyValueRecord<K, V, SRCID>> internalExecutor = new InternalExecutor<>(entityTreeQuery, connectionProvider);
		Iterables.forEachChunk(
				ids,
				dialect.getInOperatorMaxSize(),
				chunks -> {},
				chunkSize -> null,    // no particular initialization to do
				(context, chunk) -> {
					result.addAll(internalExecutor.select(chunk));
				},
				context -> {}
		);
		return result;
	}
	
	/**
	 * Small class to avoid passing {@link EntityTreeQuery} as argument to all methods
	 */
	private class InternalExecutor<ROW> {
		
		private final Query query;
		private final EntityTreeInflater<ROW> inflater;
		private final Map<Selectable<?>, ResultSetReader<?>> selectParameterBinders;
		private final Map<Selectable<?>, String> columnAliases;
		private final ConnectionProvider connectionProvider;
		
		private InternalExecutor(EntityTreeQuery<ROW> entityTreeQuery, ConnectionProvider connectionProvider) {
			this(entityTreeQuery.getQuery(), entityTreeQuery.getInflater(), entityTreeQuery.getSelectParameterBinders(), entityTreeQuery.getColumnAliases(), connectionProvider);
		}
		
		private InternalExecutor(Query query,
		                         EntityTreeInflater<ROW> inflater,
		                         Map<Selectable<?>, ? extends ResultSetReader<?>> selectParameterBinders,
		                         Map<Selectable<?>, String> columnAliases, ConnectionProvider connectionProvider) {
			this.query = query;
			this.inflater = inflater;
			this.selectParameterBinders = (Map<Selectable<?>, ResultSetReader<?>>) selectParameterBinders;
			this.columnAliases = columnAliases;
			this.connectionProvider = connectionProvider;
		}
		
		private Set<ROW> select(List<SRCID> srcIds) {
			String idsParameterName = "ids";
			if (sourceIdMapping.getIdentifierAssembler() instanceof ComposedIdentifierAssembler) {
				if (!dialect.supportsTupleCondition()) {
					throw new UnsupportedOperationException("Tuple condition is not supported by the database dialect but composite identifier requires it for 2-phases loading :"
							+ Reflections.toString(sourceIdMapping.getIdentifierType()));
				}
				Column<MAPTABLE, ?>[] array = reverseForeignKey.values().<Column<MAPTABLE, ?>>toArray(new Column[0]);
				Placeholder<SRCID, List<Object[]>> placeholder = new Placeholder<>(idsParameterName, sourceIdMapping.getIdentifierType());
				TupleIn in = new TupleIn(array, placeholder);
				Map<Column<MAPTABLE, ?>, Integer> columnIndexes = new HashMap<>();
				Map<Integer, PreparedStatementWriter<?>> psWriters = new HashMap<>();
				for (int i = 0; i < array.length; i++) {
					columnIndexes.put(array[i], i);
					psWriters.put(i, dialect.getColumnBinderRegistry().getBinder(array[i]));
				}
				
				query.getWhere().and(in);
				
				DelegatingCompositeTypeBinder<SRCID> compositeTypeBinder = new DelegatingCompositeTypeBinder<>(
						sourceIdMapping.getIdentifierType(),
						psWriters,
						new Function<SRCID, Object[]>() {
							@Override
							public Object[] apply(SRCID srcid) {
								Map<Column<LEFTTABLE, ?>, ?> identifierValues = ((ComposedIdentifierAssembler<SRCID, LEFTTABLE>) sourceIdMapping.getIdentifierAssembler()).getColumnValues(srcid);
								Map<Column<LEFTTABLE, ?>, Column<MAPTABLE, ?>> typedReverseForeignKey = (Map) reverseForeignKey;
								Map<Column<MAPTABLE, ?>, ?> columnValues = Maps.innerJoin(typedReverseForeignKey, identifierValues);
								
								Object[] objects = new Object[array.length];
								columnValues.forEach((column, columnValue) -> {
									objects[columnIndexes.get(column)] = columnValue;
								});
								return objects;
							}
						}
				);
				
//				dialect.getColumnBinderRegistry().register(sourceIdMapping.getIdentifierType(), compositeTypeBinder);
				SmartListCompositeParameterBinder<SRCID> smartListBinder = new SmartListCompositeParameterBinder<>(compositeTypeBinder);
				dialect.getColumnBinderRegistry().register(sourceIdMapping.getIdentifierType(), (ParameterBinder<SRCID>) smartListBinder);
			} else {
				SmartListParameterBinder<SRCID> smartListBinder = new SmartListParameterBinder<>(dialect.getColumnBinderRegistry().getBinder(sourceIdMapping.getIdentifierType()));
				dialect.getColumnBinderRegistry().register(sourceIdMapping.getIdentifierType(), (ParameterBinder<SRCID>) smartListBinder);
				Column<MAPTABLE, ?> pkColumn = (Column<MAPTABLE, ?>) first(reverseForeignKey.values());
				Placeholder<SRCID, List<SRCID>> placeholder = new Placeholder<>(idsParameterName, sourceIdMapping.getIdentifierType());
				In<SRCID> in = new In<>(placeholder);
				query.getWhere().and(pkColumn, in);
			}
			
			QuerySQLBuilderFactory.QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query);
			ExpandableSQLAppender preparableSQL = sqlQueryBuilder.toPreparableSQL();
			
			return execute(preparableSQL.toPreparedSQL(Maps.asMap(idsParameterName, srcIds)));
		}
		
		private <ParamType> Set<ROW> execute(SQLStatement<ParamType> query) {
			try (ReadOperation<ParamType> readOperation = dialect.getReadOperationFactory().createInstance(query, connectionProvider)) {
//				readOperation.setListener((SQLOperation.SQLOperationListener<ParamType>) operationListener);
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
	}
	
	private static class SmartListParameterBinder<C> implements ParameterBinder<Object> {
		
		protected final ParameterBinder<C> singleValueBinder;
		
		public SmartListParameterBinder(ParameterBinder<C> singleValueBinder) {
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
	
	private static class SmartListCompositeParameterBinder<C> extends SmartListParameterBinder<C> implements CompositeTypeBinder<Object> {
		
		public SmartListCompositeParameterBinder(CompositeTypeBinder<C> componentTypeBinders) {
			super(componentTypeBinders);
		}
		
		@Override
		public int getComponentTypeSize() {
			return ((CompositeTypeBinder<C>) singleValueBinder).getComponentTypeSize();
		}
	}
}
