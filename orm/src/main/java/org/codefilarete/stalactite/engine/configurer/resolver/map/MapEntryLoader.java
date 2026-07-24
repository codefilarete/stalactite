package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecord;
import org.codefilarete.stalactite.engine.configurer.map.RecordId;
import org.codefilarete.stalactite.engine.configurer.resolver.map.EntryMapResolver.KeyValueRecordPersister;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.SelectListenerCollection;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory;
import org.codefilarete.stalactite.query.model.GroupBy;
import org.codefilarete.stalactite.query.model.Having;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Where;
import org.codefilarete.stalactite.query.model.operator.In;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.result.ColumnedRowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.collection.Iterables;

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
	private EntityTreeQuery<KeyValueRecord<K, V, SRCID>> entityTreeQuery;
	private final Map<JoinLink<LEFTTABLE, ?>, JoinLink<MAPTABLE, ?>> joinLinkJoinLinkMap;
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	
	SelectListenerCollection<KeyValueRecord<K, V, SRCID>, SRCID> selectListenerCollection = new SelectListenerCollection<>();
	
	public MapEntryLoader(KeyValueRecordPersister<K, V, SRCID, MAPTABLE> keyValueRecordPersister,
	                      Map<JoinLink<LEFTTABLE, ?>, JoinLink<MAPTABLE, ?>> joinLinkJoinLinkMap,
	                      Dialect dialect,
	                      ConnectionProvider connectionProvider) {
		this.joinLinkJoinLinkMap = joinLinkJoinLinkMap;
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
		
		// Note that executor emits select listener events
		int estimatedResultSize = Iterables.size(ids);
		// we avoiding relying on Entity equals/Hashcode by using a Map based on System.identityHashCode(..)
		Set<KeyValueRecord<K, V, SRCID>> result = Collections.newSetFromMap(new EntityTreeInflater.IdentityLinkedMap<>(estimatedResultSize));
		Accumulator<KeyValueRecord<K, V, SRCID>, Set<KeyValueRecord<K, V, SRCID>>, Set<KeyValueRecord<K, V, SRCID>>> resultAccumulator = Accumulators.toCollection(() -> result);
//		if (sourceIdMapping.getIdentifierAssembler() instanceof ComposedIdentifierAssembler) {
//			// && dialect.supportTupleIn
//			Map<? extends Column<?, ?>, ?> columnValues = ((ComposedIdentifierAssembler<SRCID, ?>) sourceIdMapping.getIdentifierAssembler()).getColumnValues(ids);
//			TupleIn tupleIn = TupleIn.transformBeanColumnValuesToTupleInValues(estimatedResultSize, columnValues);
//			EntityQueryCriteriaSupport<C, I> newCriteriaSupport = newCriteriaSupport();
//			newCriteriaSupport.getEntityCriteriaSupport().getCriteria().and(tupleIn);
//			return newCriteriaSupport.wrapIntoExecutable().execute(resultAccumulator);
//		} else {
//			ReadWritePropertyAccessPoint<SRC, SRCID> criteriaAccessor;
//			if (idMapping.getIdAccessor() instanceof AccessorWrapperIdAccessor) {
//				criteriaAccessor = ((AccessorWrapperIdAccessor<SRC, SRCID>) idMapping.getIdAccessor()).getIdAccessor();
//			} else if (idMapping.getIdAccessor() instanceof KeyValueRecordIdMapping.KeyValueRecordIdAccessor) {
//				ReadWritePropertyAccessPoint<RecordId, ?> accessor = Accessors.readWriteAccessPoint(RecordId::getId);
//				criteriaAccessor = (ReadWritePropertyAccessPoint<SRC, SRCID>) accessor;
//			} else {
//				throw new UnsupportedOperationException("Unsupported id accessor type: " + idMapping.getIdAccessor().getClass());
//			}
		
		this.entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		In<SRCID> in = new In<>();
		
		Column<MAPTABLE, ?> first = (Column<MAPTABLE, ?>) Iterables.first(joinLinkJoinLinkMap.values());
		
		Query queryClone = new Query(
				new Select(entityTreeQuery.getQuery().getSelect()),
				entityTreeQuery.getQuery().getFrom(),
				new Where().and(first, in),
				new GroupBy(),
				new Having(),
				new OrderBy(),
				new Limit());
		
		QuerySQLBuilderFactory.QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(queryClone);
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparableSQL().toPreparedSQL(new HashMap<>());
		InternalExecutor<KeyValueRecord<K, V, SRCID>> internalExecutor = new InternalExecutor<>(entityTreeQuery, dialect, connectionProvider);
		
		Iterables.forEachChunk(
				ids,
				dialect.getInOperatorMaxSize(),
				chunks -> {},
				chunkSize -> null,    // no particular initialization to do
				(context, chunk) -> {
					preparedSQL.setValue(1, Iterables.first(chunk));
					Set<KeyValueRecord<K, V, SRCID>> recordSet = internalExecutor.execute(preparedSQL);
					result.addAll(recordSet);
				},
				context -> {}
		);
		
		selectListenerCollection.afterSelect(result);
		
		return result;
	}
	
	public void addSelectListener(SelectListener<KeyValueRecord<K, V, SRCID>, SRCID> selectListener) {
		selectListenerCollection.add(selectListener);
	}
	
	/**
	 * Small class to avoid passing {@link EntityTreeQuery} as argument to all methods
	 */
	private static class InternalExecutor<C> {
		
		private final EntityTreeInflater<C> inflater;
		private final Map<Selectable<?>, ResultSetReader<?>> selectParameterBinders;
		private final Map<Selectable<?>, String> columnAliases;
		private final Dialect dialect;
		private final ConnectionProvider connectionProvider;
		
		private InternalExecutor(EntityTreeQuery<C> entityTreeQuery, Dialect dialect, ConnectionProvider connectionProvider) {
			this(entityTreeQuery.getInflater(), entityTreeQuery.getSelectParameterBinders(), entityTreeQuery.getColumnAliases(), dialect, connectionProvider);
		}
		
		private InternalExecutor(EntityTreeInflater<C> inflater,
		                         Map<Selectable<?>, ? extends ResultSetReader<?>> selectParameterBinders,
		                         Map<Selectable<?>, String> columnAliases, Dialect dialect, ConnectionProvider connectionProvider) {
			this.inflater = inflater;
			this.selectParameterBinders = (Map<Selectable<?>, ResultSetReader<?>>) selectParameterBinders;
			this.columnAliases = columnAliases;
			this.dialect = dialect;
			this.connectionProvider = connectionProvider;
		}
		
		private <ParamType> Set<C> execute(SQLStatement<ParamType> query) {
			try (ReadOperation<ParamType> readOperation = dialect.getReadOperationFactory().createInstance(query, connectionProvider)) {
//				readOperation.setListener((SQLOperation.SQLOperationListener<ParamType>) operationListener);
				// Note that setValues must be done after operationListener set
				readOperation.setValues(query.getValues());
				return transform(readOperation);
			} catch (RuntimeException e) {
				throw new SQLExecutionException(query.getSQL(), e);
			}
		}
		
		private Set<C> transform(ReadOperation<?> closeableOperation) {
			ResultSet resultSet = closeableOperation.execute();
			// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
			ColumnedRowIterator rowIterator = new ColumnedRowIterator(resultSet, selectParameterBinders, columnAliases);
			return transform(rowIterator);
		}
		
		private Set<C> transform(Iterator<? extends ColumnedRow> rowIterator) {
			return inflater.transform(() -> (Iterator<ColumnedRow>) rowIterator, 50);
		}
	}
}
