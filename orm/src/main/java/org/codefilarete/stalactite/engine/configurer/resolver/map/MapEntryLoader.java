package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.codefilarete.stalactite.query.model.GroupBy;
import org.codefilarete.stalactite.query.model.Having;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Where;
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
	
	private InternalExecutor<KeyValueRecord<K, V, SRCID>> internalExecutor;
	
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
		this.internalExecutor = new InternalExecutor<>(entityTreeQuery, dialect, connectionProvider);
		Query queryClone = new Query(
				entityTreeQuery.getQuery().getSelect(),
				entityTreeQuery.getQuery().getFrom(),
				new Where(),
				new GroupBy(),
				new Having(),
				new OrderBy(),
				new Limit());
		Iterables.forEachChunk(
				ids,
				dialect.getInOperatorMaxSize(),
				chunks -> {},
				chunkSize -> null,    // no particular initialization to do
				(context, chunk) -> {
					result.addAll(selectChunk(queryClone, chunk, estimatedResultSize));
				},
				context -> {}
		);
		
		return result;
	}
	
	private Set<KeyValueRecord<K, V, SRCID>> selectChunk(Query queryClone, List<SRCID> chunk, int estimatedResultSize) {
		if (sourceIdMapping.getIdentifierAssembler() instanceof ComposedIdentifierAssembler) {
			if (!dialect.supportsTupleCondition()) {
				throw new UnsupportedOperationException("Tuple condition is not supported by the database dialect but composite identifier requires it for 2-phases loading :"
						+ Reflections.toString(sourceIdMapping.getIdentifierInsertionManager().getIdentifierType()));
			}
			Map<Column<LEFTTABLE, ?>, ?> identifierValues = ((ComposedIdentifierAssembler<SRCID, LEFTTABLE>) sourceIdMapping.getIdentifierAssembler()).getColumnValues(chunk);
			Map<Column<LEFTTABLE, ?>, Column<MAPTABLE, ?>> typedReverseForeignKey = (Map) reverseForeignKey;
			Map<Column<MAPTABLE, ?>, ?> columnValues = Maps.innerJoin(typedReverseForeignKey, identifierValues);
			TupleIn in = TupleIn.transformBeanColumnValuesToTupleInValues(estimatedResultSize, columnValues);
			queryClone.getWhere().and(in);
		} else {
			Column<MAPTABLE, ?> pkColumn = (Column<MAPTABLE, ?>) first(reverseForeignKey.values());
			In<?> in = new In<>(chunk);
			queryClone.getWhere().and(pkColumn, in);
		}
		
		QuerySQLBuilderFactory.QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(queryClone);
		ExpandableSQLAppender preparableSQL = sqlQueryBuilder.toPreparableSQL();
		
		return internalExecutor.execute(preparableSQL.toPreparedSQL(new HashMap<>()));
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
