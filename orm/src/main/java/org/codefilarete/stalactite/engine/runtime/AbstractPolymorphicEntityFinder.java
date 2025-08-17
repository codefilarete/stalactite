package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.query.EntityFinder;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.GroupBy;
import org.codefilarete.stalactite.query.model.Having;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Where;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.result.ColumnedRowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.collection.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parent class for polymorphic entity selection.
 * Made to share code between polymorphic cases.
 * 
 * @param <C>
 * @param <I>
 * @param <T>
 * @author Guillaume Mary
 */
public abstract class AbstractPolymorphicEntityFinder<C, I, T extends Table<T>> implements EntityFinder<C, I> {
	
	protected final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	
	protected final EntityJoinTree<C, I> mainEntityJoinTree;
	protected final Map<Class<C>, ConfiguredRelationalPersister<C, I>> persisterPerSubclass;
	protected final ConnectionProvider connectionProvider;
	protected final Dialect dialect;
	protected final boolean hasSubPolymorphicPersister;
	private final AccessorChain<C, I> entityIdAccessor;
	
	private SQLOperationListener<?> operationListener;
	
	protected AbstractPolymorphicEntityFinder(
			ConfiguredRelationalPersister<C, I> mainPersister,
			Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass,
			ConnectionProvider connectionProvider,
			Dialect dialect) {
		this.mainEntityJoinTree = mainPersister.getEntityJoinTree();
		this.persisterPerSubclass = (Map<Class<C>, ConfiguredRelationalPersister<C, I>>) persisterPerSubclass;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
		this.hasSubPolymorphicPersister = Iterables.find(persisterPerSubclass.values(), subPersister -> subPersister instanceof AbstractPolymorphismPersister) != null;
		AccessorWrapperIdAccessor<C, I> idAccessor = (AccessorWrapperIdAccessor<C, I>) mainPersister.<T>getMapping().getIdMapping().getIdAccessor();
		this.entityIdAccessor = new AccessorChain<>(idAccessor.getIdAccessor());
	}
	
	@Override
	public void setOperationListener(SQLOperationListener<?> operationListener) {
		this.operationListener = operationListener;
	}
	
	@Override
	public Set<C> select(ConfiguredEntityCriteria where, OrderBy orderBy, Limit limit, Map<String, Object> valuesPerParam) {
		if (where.hasCollectionCriteria()) {
			return selectIn2Phases(where, orderBy, limit);
		} else {
			return selectWithSingleQuery(where, orderBy, limit);
		}
	}
	
	public abstract Set<C> selectIn2Phases(ConfiguredEntityCriteria where, OrderBy orderBy, Limit limit);
	
	public abstract Set<C> selectWithSingleQuery(ConfiguredEntityCriteria where, OrderBy orderBy, Limit limit);
	
	protected abstract EntityTreeQuery<C> getAggregateQueryTemplate();
	
	/**
	 * Method to avoid code duplication in subclasses.
	 * This is used each time a load in 2-phases is done and the ids of the entities (that match user's conditions) have been retrieved from the
	 * database.
	 * @param ids the entity identifiers to be selected (may be empty)
	 * @return a {@link Set} of entities loaded by the given identifiers
	 */
	protected Set<C> selectWithSingleQueryWhereIdIn(Iterable<I> ids) {
		if (!Iterables.isEmpty(ids)) {
			// the newCriteriaSupport() will create a copy of the main entity criteria, so we can modify it without affecting the main one
			Query query = getAggregateQueryTemplate().getQuery();
			EntityCriteriaSupport<C> and = newCriteriaSupport().getEntityCriteriaSupport().and(entityIdAccessor, Operators.in(ids));
			Query queryClone = new Query(query.getSelectDelegate(), query.getFromDelegate(), new Where<>().add(and.getCriteria()), new GroupBy(), new Having(),
					new OrderBy(), // No order-by since we are in a Collection criteria, sort we'll be made downstream in memory see EntityCriteriaSupport#wrapGraphload()
					new Limit() // No limit since we already limited our result through the selection of the ids
			);
			return selectWithSingleQuery(queryClone, getAggregateQueryTemplate(), dialect, connectionProvider);
		} else {
			return Collections.emptySet();
		}
	}
	
	/**
	 * A reusable method that execute query build from give {@link EntityJoinTree} with query clauses given as argument
	 * @param queryClone the {@link Query} to execute
	 * @param entityTreeQuery the tree representing the way to build the final aggregate
	 * @param dialect the dialect helping to get the right adaption layer to the database
	 * @param connectionProvider the connection provider
	 * @return a {@link Set} of loaded entities according to given criteria
	 */
	protected Set<C> selectWithSingleQuery(Query queryClone,
										   EntityTreeQuery<C> entityTreeQuery,
										   Dialect dialect,
										   ConnectionProvider connectionProvider) {
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(queryClone);
		
		EntityTreeInflater<C> inflater = entityTreeQuery.getInflater();
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparableSQL().toPreparedSQL(new HashMap<>());
		try (ReadOperation<Integer> readOperation = dialect.getReadOperationFactory().createInstance(preparedSQL, connectionProvider)) {
			readOperation.setListener((SQLOperationListener<Integer>) operationListener);
			ResultSet resultSet = readOperation.execute();
			// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
			Iterator<? extends ColumnedRow> rowIterator = new ColumnedRowIterator(resultSet, entityTreeQuery.getSelectParameterBinders(), entityTreeQuery.getColumnAliases());
			return inflater.transform(() -> (Iterator<ColumnedRow>) rowIterator, 50);
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	@Override
	public <R, O> R selectProjection(Consumer<Select> selectAdapter,
									 Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator,
									 ConfiguredEntityCriteria where,
									 boolean distinct,
									 OrderBy orderBy,
									 Limit limit) {
		Query queryClone = new Query(new Select(), getAggregateQueryTemplate().getQuery().getFromDelegate(), new Where<>(where.getCriteria()), new GroupBy(), new Having(), orderBy, limit);
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(queryClone);
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		selectAdapter.accept(queryClone.getSelectDelegate());
		Map<Selectable<?>, ResultSetReader<?>> columnReaders = Iterables.map(queryClone.getColumns(), Function.identity(), selectable -> dialect.getColumnBinderRegistry().getBinder(selectable.getJavaType()));
		
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparableSQL().toPreparedSQL(new HashMap<>());
		return readProjection(preparedSQL, columnReaders, queryClone.getAliases(), accumulator);
	}
	
	protected <R, O> R readProjection(PreparedSQL preparedSQL, Map<Selectable<?>, ResultSetReader<?>> columnReaders, Map<Selectable<?>, String> aliases, Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator) {
		try (ReadOperation<Integer> closeableOperation = dialect.getReadOperationFactory().createInstance(preparedSQL, connectionProvider)) {
			ColumnedRowIterator rowIterator = new ColumnedRowIterator(closeableOperation.execute(), columnReaders, aliases);
			return accumulator.collect(Iterables.stream(rowIterator).map(row -> (Function<Selectable<O>, O>) row::get).collect(Collectors.toList()));
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
}
