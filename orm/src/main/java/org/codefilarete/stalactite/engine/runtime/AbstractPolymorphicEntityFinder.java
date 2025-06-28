package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.EntityFinder;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.LimitAware;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.OrderByChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
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
	private final EntityMapping<C, I, T> mainMapping;
	
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
		this.mainMapping = mainPersister.getMapping();
		AccessorWrapperIdAccessor<C, I> idAccessor = (AccessorWrapperIdAccessor<C, I>) mainMapping.getIdMapping().getIdAccessor();
		this.entityIdAccessor = new AccessorChain<>(idAccessor.getIdAccessor());
	}
	
	@Override
	public Set<C> select(ConfiguredEntityCriteria where, Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer, Map<String, Object> valuesPerParam) {
		if (where.hasCollectionCriteria()) {
			return selectIn2Phases(where, orderByClauseConsumer, limitAwareConsumer);
		} else {
			return selectWithSingleQuery(where, orderByClauseConsumer, limitAwareConsumer);
		}
	}
	
	public abstract Set<C> selectIn2Phases(ConfiguredEntityCriteria where, Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer);
	
	public abstract Set<C> selectWithSingleQuery(ConfiguredEntityCriteria where,
								 Consumer<OrderByChain<?>> orderByClauseConsumer,
								 Consumer<LimitAware<?>> limitAwareConsumer);
	
	protected EntityCriteriaSupport<C> newWhereIdClause(Iterable<I> ids) {
		// Because we only need the id in the where clause, we don't need persister's EntityCriteriaSupport that contains all
		// the potential criteria on relations, hence we can instantiate a new one for our local need
		return new EntityCriteriaSupport<>(mainMapping)
				.and(entityIdAccessor, Operators.in(ids));
	}
	
	/**
	 * A reusable method that execute query build from give {@link EntityJoinTree} with query clauses given as argument
	 * @param where the conditions to apply to the entity query
	 * @param orderByClauseConsumer an adapter of the order by clause
	 * @param limitAwareConsumer an adapter of the limit clause
	 * @param entityJoinTree the tree representing entity joins
	 * @param dialect the dialect helping to get the right adaption layer to the database
	 * @param connectionProvider the connection provider
	 * @return a {@link Set} of loaded entities according to given criteria
	 */
	protected Set<C> selectWithSingleQuery(ConfiguredEntityCriteria where,
										   Consumer<OrderByChain<?>> orderByClauseConsumer,
										   Consumer<LimitAware<?>> limitAwareConsumer,
										   EntityJoinTree<C, I> entityJoinTree,
										   Dialect dialect,
										   ConnectionProvider connectionProvider) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		orderByClauseConsumer.accept(new ColumnCloneAwareOrderBy(query.orderBy(), entityTreeQuery.getColumnClones()));
		limitAwareConsumer.accept(query.orderBy());
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where.getCriteria(), entityTreeQuery.getColumnClones());
		
		EntityTreeInflater<C> inflater = entityTreeQuery.getInflater();
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparableSQL().toPreparedSQL(new HashMap<>());
		try (ReadOperation<Integer> readOperation = dialect.getReadOperationFactory().createInstance(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
			RowIterator rowIterator = new RowIterator(resultSet, entityTreeQuery.getSelectParameterBinders());
			return inflater.transform(() -> rowIterator, 50);
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	@Override
	public <R, O> R selectProjection(Consumer<Select> selectAdapter, Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator, CriteriaChain where,
									 boolean distinct, Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(this.mainEntityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		query.getSelectDelegate().setDistinct(distinct);
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where, entityTreeQuery.getColumnClones());
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		selectAdapter.accept(query.getSelectDelegate());
		Map<Selectable<?>, String> aliases = query.getAliases();
		ColumnedRow columnedRow = new ColumnedRow(aliases::get);
		
		Map<String, ResultSetReader<?>> columnReaders = Iterables.map(query.getColumns(), new AliasAsserter<>(aliases::get), selectable -> dialect.getColumnBinderRegistry().getBinder(selectable.getJavaType()));
		
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparableSQL().toPreparedSQL(new HashMap<>());
		return readProjection(preparedSQL, columnReaders, columnedRow, accumulator);
	}
	
	protected <R, O> R readProjection(PreparedSQL preparedSQL, Map<String, ResultSetReader<?>> columnReaders, ColumnedRow columnedRow, Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator) {
		try (ReadOperation<Integer> closeableOperation = dialect.getReadOperationFactory().createInstance(preparedSQL, connectionProvider)) {
			RowIterator rowIterator = new RowIterator(closeableOperation.execute(), columnReaders);
			return accumulator.collect(Iterables.stream(rowIterator).map(row -> (Function<Selectable<O>, O>) selectable -> columnedRow.getValue(selectable, row)).collect(Collectors.toList()));
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	/**
	 * Small class that will be used to ensure that a {@link Selectable} as an alias in the query
	 * @param <S>
	 * @author Guillaume Mary
	 */
	protected static class AliasAsserter<S extends Selectable> implements Function<S, String> {
		
		private final Function<S, String> delegate;
		
		public AliasAsserter(Function<S, String> delegate) {
			this.delegate = delegate;
		}
		
		@Override
		public String apply(S selectable) {
			String alias = delegate.apply(selectable);
			if (alias == null) {
				throw new IllegalArgumentException("Item " + selectable.getExpression() + " must have an alias");
			}
			return alias;
		}
	}
}
