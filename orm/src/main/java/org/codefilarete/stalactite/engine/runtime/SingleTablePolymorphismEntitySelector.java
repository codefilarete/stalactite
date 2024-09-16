package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.engine.runtime.load.JoinTableRootJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.SingleTableRootJoinNode;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.LimitAware;
import org.codefilarete.stalactite.query.model.OrderByChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphismEntitySelector<C, I, T extends Table<T>, DTYPE> extends AbstractPolymorphicEntitySelector<C, I, T> {
	
	@VisibleForTesting
	static final String DISCRIMINATOR_ALIAS = "DISCRIMINATOR";
	
	private final IdentifierAssembler<I, T> identifierAssembler;
	private final Column<T, DTYPE> discriminatorColumn;
	private final SingleTablePolymorphism<C, DTYPE> polymorphismPolicy;
	private final EntityJoinTree<C, I> singleLoadEntityJoinTree;
	
	public SingleTablePolymorphismEntitySelector(ConfiguredRelationalPersister<C, I> mainPersister,
												 Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass,
												 Column<T, DTYPE> discriminatorColumn,
												 SingleTablePolymorphism<C, DTYPE> polymorphismPolicy,
												 ConnectionProvider connectionProvider,
												 Dialect dialect) {
		super(mainPersister.getEntityJoinTree(), persisterPerSubclass, connectionProvider, dialect);
		this.identifierAssembler = mainPersister.getMapping().getIdMapping().getIdentifierAssembler();
		this.discriminatorColumn = discriminatorColumn;
		this.polymorphismPolicy = polymorphismPolicy;
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
	Set<C> selectWithSingleQuery(ConfiguredEntityCriteria where, Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer) {
		return super.selectWithSingleQuery(where, orderByClauseConsumer, limitAwareConsumer, singleLoadEntityJoinTree, dialect, connectionProvider);
	}
	
	@Override
	Set<C> selectIn2Phases(ConfiguredEntityCriteria where, Consumer<OrderByChain<?>> orderByClauseConsumer, Consumer<LimitAware<?>> limitAwareConsumer) {
		EntityTreeQuery<C> entityTreeQuery = new EntityTreeQueryBuilder<>(mainEntityJoinTree, dialect.getColumnBinderRegistry()).buildSelectQuery();
		Query query = entityTreeQuery.getQuery();
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where.getCriteria(), entityTreeQuery.getColumnClones());
		
		// First phase : selecting ids (made by clearing selected elements for performance issue)
		query.getSelectSurrogate().clear();
		PrimaryKey<T, I> pk = ((T) mainEntityJoinTree.getRoot().getTable()).getPrimaryKey();
		pk.getColumns().forEach(column -> query.select(column, column.getAlias()));
		query.select(discriminatorColumn, DISCRIMINATOR_ALIAS);
		
		// selecting ids and their entity type
		Map<String, ResultSetReader> columnReaders = new HashMap<>();
		Map<Selectable<?>, String> aliases = query.getAliases();
		aliases.forEach((selectable, s) -> columnReaders.put(s, dialect.getColumnBinderRegistry().getBinder((Column) selectable)));
		ColumnedRow columnedRow = new ColumnedRow(aliases::get);
		orderByClauseConsumer.accept(query.orderBy());
		limitAwareConsumer.accept(query.orderBy());
		
		Map<Class, Set<I>> idsPerSubclass = readIds(sqlQueryBuilder.toPreparedSQL(), columnReaders, columnedRow);
		
		// Second phase : selecting entities by delegating it to each subclass loader
		// It will generate 1 query per found subclass, made as this :
		// - to avoid superfluous join and complex query in case of relation
		// - make it simpler to implement
		Set<C> result = new HashSet<>();
		idsPerSubclass.forEach((subClass, subEntityIds) -> result.addAll(persisterPerSubclass.get(subClass).select(subEntityIds)));
		return result;
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
