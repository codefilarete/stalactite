package org.codefilarete.stalactite.engine.configurer.resolver.elementcollection;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.engine.configurer.resolver.elementcollection.ElementCollectionResolver.ElementRecordPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.IdentityLinkedMap;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
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
 * Particular {@link SelectExecutor} that loads {@link ElementRecord} from a collection table by the source identifier.
 * Made for the separate loading of a {@link Collection} elements (as {@link ElementRecord}) by the source identifier : we
 * could have done it through the {@link ElementRecordPersister}, but its select is made through {@link ElementRecord}
 * which we don't have on separate loading since we only have the left entity identifiers available, whereas
 * {@link ElementRecord} is made of the left entity identifier and the element value.
 * 
 * @param <SRC>
 * @param <SRCID>
 * @param <TRGT>
 * @param <LEFTTABLE>
 * @param <COLLECTIONTABLE>
 * @author Guillaume Mary
 */
public class ElementCollectionLoader<SRC, SRCID, TRGT, LEFTTABLE extends Table<LEFTTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>> implements SelectExecutor<ElementRecord<TRGT, SRCID>, SRCID> {
	
	private final EntityJoinTree<ElementRecord<TRGT, SRCID>, ElementRecord<TRGT, SRCID>> entityJoinTree;
	private final IdMapping<SRC, SRCID> sourceIdMapping;
	private final Map<JoinLink<LEFTTABLE, ?>, JoinLink<COLLECTIONTABLE, ?>> reverseForeignKey;
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	
	private InternalExecutor<ElementRecord<TRGT, SRCID>> internalExecutor;
	
	public ElementCollectionLoader(IdMapping<SRC, SRCID> sourceIdMapping,
	                               ElementRecordPersister<TRGT, SRCID, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>> collectionPersister,
	                               Map<JoinLink<LEFTTABLE, ?>, JoinLink<COLLECTIONTABLE, ?>> reverseForeignKey,
	                               Dialect dialect,
	                               ConnectionProvider connectionProvider) {
		this.sourceIdMapping = sourceIdMapping;
		this.reverseForeignKey = reverseForeignKey;
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
		this.entityJoinTree = new EntityJoinTree<>(collectionPersister.getMapping());
	}
	
	@Override
	public Set<ElementRecord<TRGT, SRCID>> select(Iterable<SRCID> ids) {
		int estimatedResultSize = Iterables.size(ids);
		// we avoid relying on Entity equals/Hashcode by using a Map based on System.identityHashCode(..)
		Set<ElementRecord<TRGT, SRCID>> result = Collections.newSetFromMap(new IdentityLinkedMap<>(estimatedResultSize));
		EntityTreeQuery<ElementRecord<TRGT, SRCID>> entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
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
	
	private Set<ElementRecord<TRGT, SRCID>> selectChunk(Query queryClone, List<SRCID> chunk, int estimatedResultSize) {
		if (sourceIdMapping.getIdentifierAssembler() instanceof ComposedIdentifierAssembler) {
			if (!dialect.supportsTupleCondition()) {
				throw new UnsupportedOperationException("Tuple condition is not supported by the database dialect but composite identifier requires it for 2-phases loading :"
						+ Reflections.toString(sourceIdMapping.getIdentifierType()));
			}
			Map<Column<LEFTTABLE, ?>, ?> identifierValues = ((ComposedIdentifierAssembler<SRCID, LEFTTABLE>) sourceIdMapping.getIdentifierAssembler()).getColumnValues(chunk);
			Map<Column<LEFTTABLE, ?>, Column<COLLECTIONTABLE, ?>> typedReverseForeignKey = (Map) reverseForeignKey;
			Map<Column<COLLECTIONTABLE, ?>, ?> columnValues = Maps.innerJoin(typedReverseForeignKey, identifierValues);
			TupleIn in = TupleIn.transformBeanColumnValuesToTupleInValues(estimatedResultSize, columnValues);
			queryClone.getWhere().and(in);
		} else {
			Column<COLLECTIONTABLE, ?> pkColumn = (Column<COLLECTIONTABLE, ?>) first(reverseForeignKey.values());
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
