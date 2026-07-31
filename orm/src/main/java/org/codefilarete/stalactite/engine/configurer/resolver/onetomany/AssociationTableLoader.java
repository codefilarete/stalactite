package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.IdentityLinkedMap;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.EntityMapping;
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
 * {@link SelectExecutor} that loads the records of an association table from the identifiers of the entities that
 * own the relation (the "left" or "source" side of the association).
 * <p>
 * It owns its own {@link EntityJoinTree} whose root is the association table, which makes it the entry point of any
 * second-phase (separate fetch) loading : the aggregate main query doesn't have to join the association table
 * anymore, which prevents it from returning a cartesian product of all the relations of the aggregate.
 * <p>
 * Note that records are not selected through the association table persister because its select is made from the
 * record identifier (which is the whole association row), whereas on a separate loading we only have the source
 * entity identifiers at hand.
 *
 * @param <C> record type (association row bean)
 * @param <ID> record identifier type
 * @param <SRC> type of the entity owning the relation
 * @param <SRCID> identifier type of the entity owning the relation
 * @param <LEFTTABLE> table of the entity owning the relation
 * @param <ASSOCIATIONTABLE> association table type
 * @author Guillaume Mary
 */
public class AssociationTableLoader<C, ID, SRC, SRCID, LEFTTABLE extends Table<LEFTTABLE>, ASSOCIATIONTABLE extends Table<ASSOCIATIONTABLE>> implements SelectExecutor<C, SRCID> {
	
	private final EntityJoinTree<C, ID> entityJoinTree;
	private final IdMapping<SRC, SRCID> sourceIdMapping;
	private final Map<JoinLink<LEFTTABLE, ?>, JoinLink<ASSOCIATIONTABLE, ?>> reverseForeignKey;
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	
	private InternalExecutor<C> internalExecutor;
	
	/**
	 * @param sourceIdMapping identifier mapping of the entity owning the relation, used to build the where clause of the select
	 * @param associationRecordMapping mapping of the association table records, used as the root of the internal {@link EntityJoinTree}
	 * @param reverseForeignKey mapping between the columns of the source entity table and those of the association table
	 * @param dialect the dialect to build and execute the query
	 * @param connectionProvider the provider of the connection the query will be executed on
	 */
	public AssociationTableLoader(IdMapping<SRC, SRCID> sourceIdMapping,
	                              EntityMapping<C, ID, ASSOCIATIONTABLE> associationRecordMapping,
	                              Map<JoinLink<LEFTTABLE, ?>, JoinLink<ASSOCIATIONTABLE, ?>> reverseForeignKey,
	                              Dialect dialect,
	                              ConnectionProvider connectionProvider) {
		this.sourceIdMapping = sourceIdMapping;
		this.reverseForeignKey = reverseForeignKey;
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
		this.entityJoinTree = new EntityJoinTree<>(associationRecordMapping);
	}
	
	/**
	 * Gives the join tree of this loader : its root is the association table, hence any relation that must be loaded
	 * along with the association records has to be grafted on it.
	 *
	 * @return the join tree owned by this loader
	 */
	public EntityJoinTree<C, ID> getEntityJoinTree() {
		return entityJoinTree;
	}
	
	@Override
	public Set<C> select(Iterable<SRCID> ids) {
		int estimatedResultSize = Iterables.size(ids);
		// we avoid relying on Entity equals/Hashcode by using a Map based on System.identityHashCode(..)
		Set<C> result = Collections.newSetFromMap(new IdentityLinkedMap<>(estimatedResultSize));
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
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
	
	private Set<C> selectChunk(Query queryClone, List<SRCID> chunk, int estimatedResultSize) {
		if (sourceIdMapping.getIdentifierAssembler() instanceof ComposedIdentifierAssembler) {
			if (!dialect.supportsTupleCondition()) {
				throw new UnsupportedOperationException("Tuple condition is not supported by the database dialect but composite identifier requires it for 2-phases loading :"
						+ Reflections.toString(sourceIdMapping.getIdentifierInsertionManager().getIdentifierType()));
			}
			Map<Column<LEFTTABLE, ?>, ?> identifierValues = ((ComposedIdentifierAssembler<SRCID, LEFTTABLE>) sourceIdMapping.getIdentifierAssembler()).getColumnValues(chunk);
			Map<Column<LEFTTABLE, ?>, Column<ASSOCIATIONTABLE, ?>> typedReverseForeignKey = (Map) reverseForeignKey;
			Map<Column<ASSOCIATIONTABLE, ?>, ?> columnValues = Maps.innerJoin(typedReverseForeignKey, identifierValues);
			TupleIn in = TupleIn.transformBeanColumnValuesToTupleInValues(estimatedResultSize, columnValues);
			queryClone.getWhere().and(in);
		} else {
			Column<ASSOCIATIONTABLE, ?> pkColumn = (Column<ASSOCIATIONTABLE, ?>) first(reverseForeignKey.values());
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
