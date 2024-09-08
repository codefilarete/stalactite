package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.engine.runtime.load.JoinTableRootJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.SingleTableRootJoinNode;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.EntitySelector;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.LimitAware;
import org.codefilarete.stalactite.query.model.OrderByChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphismEntitySelector<C, I, T extends Table<T>, DTYPE> implements EntitySelector<C, I> {
	
	@VisibleForTesting
	static final String DISCRIMINATOR_ALIAS = "DISCRIMINATOR";
	
	private final IdentifierAssembler<I, T> identifierAssembler;
	private final Map<Class<C>, ConfiguredRelationalPersister<C, I>> persisterPerSubclass;
	private final Column<T, DTYPE> discriminatorColumn;
	private final SingleTablePolymorphism<C, DTYPE> polymorphismPolicy;
	private final EntityJoinTree<C, I> entityJoinTree;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	private final EntityJoinTree<C, I> singleLoadEntityJoinTree;
	
	public SingleTablePolymorphismEntitySelector(ConfiguredRelationalPersister<C, I> mainPersister,
												 Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass,
												 Column<T, DTYPE> discriminatorColumn,
												 SingleTablePolymorphism<C, DTYPE> polymorphismPolicy,
												 ConnectionProvider connectionProvider,
												 Dialect dialect) {
		this.identifierAssembler = mainPersister.getMapping().getIdMapping().getIdentifierAssembler();
		this.persisterPerSubclass = (Map<Class<C>, ConfiguredRelationalPersister<C, I>>) persisterPerSubclass;
		this.discriminatorColumn = discriminatorColumn;
		this.polymorphismPolicy = polymorphismPolicy;
		this.entityJoinTree = mainPersister.getEntityJoinTree();
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
		this.singleLoadEntityJoinTree = buildSingleLoadEntityJoinTree(mainPersister, persisterPerSubclass);
	}
	
	private SingleLoadEntityJoinTree<C, I, DTYPE> buildSingleLoadEntityJoinTree(ConfiguredRelationalPersister<C, I> mainPersister, Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass) {
		SingleLoadEntityJoinTree<C, I, DTYPE> result = new SingleLoadEntityJoinTree<>(
				mainPersister,
				new HashSet<>(persisterPerSubclass.values()),
				discriminatorColumn,
				polymorphismPolicy
		);
		// we project main persister tree to keep its relations
		mainPersister.getEntityJoinTree().projectTo(result, ROOT_STRATEGY_NAME);
		return result;
	}
	
	@Override
	public Set<C> select(ConfiguredEntityCriteria where, Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer) {
		if (where.hasCollectionCriteria()) {
			return selectIn2Phases(where);
		} else {
			return selectWithSingleQuery(where);
		}
	}
	
	private Set<C> selectWithSingleQuery(ConfiguredEntityCriteria where) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(singleLoadEntityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where.getCriteria(), entityTreeQuery.getColumnClones());
		
		EntityTreeInflater<C> inflater = entityTreeQuery.getInflater();
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL();
		try (ReadOperation<Integer> readOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			// NB: we give the same ParametersBinders of those given at ColumnParameterizedSelect since the row iterator is expected to read column from it
			RowIterator rowIterator = new RowIterator(resultSet, entityTreeQuery.getSelectParameterBinders());
			return inflater.transform(() -> rowIterator, 50);
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	private Set<C> selectIn2Phases(ConfiguredEntityCriteria where) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where.getCriteria(), entityTreeQuery.getColumnClones());
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		query.getSelectSurrogate().clear();
		PrimaryKey<T, I> pk = ((T) entityJoinTree.getRoot().getTable()).getPrimaryKey();
		pk.getColumns().forEach(column -> query.select(column, column.getAlias()));
		query.select(discriminatorColumn, DISCRIMINATOR_ALIAS);
		
		// selecting ids and their entity type
		Map<String, ResultSetReader> columnReaders = new HashMap<>();
		Map<Selectable<?>, String> aliases = query.getAliases();
		aliases.forEach((selectable, s) -> columnReaders.put(s, dialect.getColumnBinderRegistry().getBinder((Column) selectable)));
		ColumnedRow columnedRow = new ColumnedRow(aliases::get);
		
		Map<Class, Set<I>> idsPerSubclass = readIds(sqlQueryBuilder.toPreparedSQL(), columnReaders, columnedRow);
		
		// Second phase : selecting entities by delegating it to each subclass loader
		// It will generate 1 query per found subclass, made as this :
		// - to avoid superfluous join and complex query in case of relation
		// - make it simpler to implement
		Set<C> result = new HashSet<>();
		idsPerSubclass.forEach((subClass, subEntityIds) -> result.addAll(persisterPerSubclass.get(subClass).select(subEntityIds)));
		return result;
	}
	
	@Override
	public <R, O> R selectProjection(Consumer<Select> selectAdapter, Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator, CriteriaChain where,
									 boolean distinct, Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(this.entityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		query.getSelectSurrogate().setDistinct(distinct);
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where, entityTreeQuery.getColumnClones());
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		selectAdapter.accept(query.getSelectSurrogate());
		Map<Selectable<?>, String> aliases = query.getAliases();
		ColumnedRow columnedRow = new ColumnedRow(aliases::get);
		
		Map<String, ResultSetReader<?>> columnReaders = Iterables.map(query.getColumns(), new AliasAsserter<>(aliases::get), selectable -> dialect.getColumnBinderRegistry().getBinder(selectable.getJavaType()));
		
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL();
		return readProjection(preparedSQL, columnReaders, columnedRow, accumulator);
	}
	
	private <R, O> R readProjection(PreparedSQL preparedSQL, Map<String, ResultSetReader<?>> columnReaders, ColumnedRow columnedRow, Accumulator<? super Function<Selectable<O>, O>, Object, R> accumulator) {
		try (ReadOperation<Integer> closeableOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, columnReaders);
			return accumulator.collect(Iterables.stream(rowIterator).map(row -> (Function<Selectable<O>, O>) selectable -> columnedRow.getValue(selectable, row)).collect(Collectors.toList()));
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	private Map<Class, Set<I>> readIds(PreparedSQL preparedSQL, Map<String, ResultSetReader> columnReaders, ColumnedRow columnedRow) {
		try (ReadOperation<Integer> closeableOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, columnReaders);
			// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
			// impacts but very difficult to measure
			Map<Class, Set<I>> result = new KeepOrderMap<>();
			rowIterator.forEachRemaining(row -> {
				DTYPE dtype = (DTYPE) row.get(DISCRIMINATOR_ALIAS);
				I id = identifierAssembler.assemble(row, columnedRow);
				result.computeIfAbsent(polymorphismPolicy.getClass(dtype), k -> new KeepOrderSet<>()).add(id);
			});
			return result;
		} catch (RuntimeException e) {
			throw new SQLExecutionException(preparedSQL.getSQL(), e);
		}
	}
	
	/**
	 * Small class that will be used to ensure that a {@link Selectable} as an alias in the query
	 * @param <S>
	 * @author Guillaume Mary
	 */
	private static class AliasAsserter<S extends Selectable> implements Function<S, String> {
		
		private final Function<S, String> delegate;
		
		private AliasAsserter(Function<S, String> delegate) {
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
	
	/**
	 * Appropriate {@link EntityJoinTree} to instantiate {@link JoinTableRootJoinNode} as root in order to handle join-node polymorphism of root entity. 
	 * @param <C>
	 * @param <I>
	 * @author Guillaume Mary
	 */
	private static class SingleLoadEntityJoinTree<C, I, DTYPE> extends EntityJoinTree<C, I> {
		
		public <T extends Table<T>> SingleLoadEntityJoinTree(ConfiguredRelationalPersister<C, I> mainPersister,
															 Set<? extends ConfiguredRelationalPersister<C, I>> subPersisters,
															 Column<T, DTYPE> discriminatorColumn,
															 SingleTablePolymorphism<C, DTYPE> polymorphismPolicy) {
			super(self -> new SingleTableRootJoinNode<>(
					self,
					mainPersister,
					subPersisters,
					discriminatorColumn,
					polymorphismPolicy)
			);
		}
	}
}
