package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeQueryBuilder.EntityTreeQuery;
import org.codefilarete.stalactite.engine.runtime.load.JoinTableRootJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode.MergeJoinRowConsumer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.query.ConfiguredEntityCriteria;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.LimitAware;
import org.codefilarete.stalactite.query.model.OrderByChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.KeepOrderMap;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public class JoinTablePolymorphismEntitySelector<C, I, T extends Table<T>> extends AbstractPolymorphicEntitySelector<C, I, T> {
	
	private final T mainTable;
	private final SingleLoadEntityJoinTree<C, I> singleLoadEntityJoinTree;
	
	public JoinTablePolymorphismEntitySelector(
			ConfiguredRelationalPersister<C, I> mainPersister,
			Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass,
			ConnectionProvider connectionProvider,
			Dialect dialect) {
		super(mainPersister.getEntityJoinTree(), persisterPerSubclass, connectionProvider, dialect);
		this.mainTable = (T) mainPersister.getMainTable();
		this.singleLoadEntityJoinTree = buildSingleLoadEntityJoinTree(mainPersister, persisterPerSubclass);
	}
	
	private SingleLoadEntityJoinTree<C, I> buildSingleLoadEntityJoinTree(ConfiguredRelationalPersister<C, I> mainPersister, Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> persisterPerSubclass) {
		SingleLoadEntityJoinTree<C, I> result = new SingleLoadEntityJoinTree<>(
				mainPersister,
				new HashSet<>(persisterPerSubclass.values())
		);
		// sub entities persisters will be used to create entities
		persisterPerSubclass.forEach((type, persister) -> result.addMergeJoin(
				ROOT_STRATEGY_NAME,
				new EntityMergerAdapter<>(persister.<T>getMapping()),
				mainPersister.getMainTable().getPrimaryKey(),
				persister.getMainTable().getPrimaryKey(),
				JoinType.OUTER,
				columnedRow -> {
					// implemented to add newly created sub consumer to root one, therefore it will be able to create the right sub-instance
					MergeJoinRowConsumer<C> subEntityConsumer = new MergeJoinRowConsumer<>(persister.getMapping().copyTransformerWithAliases(columnedRow));
					((JoinTableRootJoinNode) result.getRoot()).addSubPersister(persister, subEntityConsumer, columnedRow);
					return subEntityConsumer;
				}
		));
		// we project main persister tree to keep its relations
		mainEntityJoinTree.projectTo(result, ROOT_STRATEGY_NAME);
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
		persisterPerSubclass.values().forEach(subclassPersister -> {
			query.getFrom().leftOuterJoin(mainTable.getPrimaryKey(), subclassPersister.getMainTable().getPrimaryKey());
			((T) subclassPersister.getMainTable()).getPrimaryKey().getColumns().forEach(column -> {
				query.select(column, column.getAlias());
			});
		});
		
		QuerySQLBuilder sqlQueryBuilder = dialect.getQuerySQLBuilderFactory().queryBuilder(query, where.getCriteria(), entityTreeQuery.getColumnClones());
		
		// selecting ids and their entity type
		Map<String, ResultSetReader> columnReaders = new HashMap<>();
		Map<Selectable<?>, String> aliases = query.getAliases();
		aliases.forEach((selectable, s) -> columnReaders.put(s, dialect.getColumnBinderRegistry().getBinder((Column) selectable)));
		ColumnedRow columnedRow = new ColumnedRow(aliases::get);
		orderByClauseConsumer.accept(query.orderBy());
		
		Map<Class, Set<I>> idsPerSubtype = readIds(sqlQueryBuilder.toPreparedSQL(), columnReaders, columnedRow);
		
		Set<C> result = new HashSet<>();
		idsPerSubtype.forEach((k, v) -> result.addAll(persisterPerSubclass.get(k).select(v)));
		return result;
	}
	
	private Map<Class, Set<I>> readIds(PreparedSQL preparedSQL, Map<String, ResultSetReader> columnReaders, ColumnedRow columnedRow) {
		// Below we keep order of given entities mainly to get steady unit tests. Meanwhile, this may have performance
		// impacts but very difficult to measure
		Map<Class, Set<I>> result = new KeepOrderMap<>();
		try (ReadOperation readOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			
			RowIterator resultSetIterator = new RowIterator(resultSet, columnReaders);
			resultSetIterator.forEachRemaining(row -> {
				
				// looking for entity type on row : we read each subclass PK and check for nullity. The non-null one is the 
				// right one
				Set<Entry<Class<C>, ConfiguredRelationalPersister<C, I>>> entries = persisterPerSubclass.entrySet();
				Duo<Class, I> duo = null;
				I identifier;
				for (Entry<Class<C>, ConfiguredRelationalPersister<C, I>> entry : entries) {
					identifier = entry.getValue().getMapping().getIdMapping().getIdentifierAssembler().assemble(row, columnedRow);
					if (identifier != null) {
						duo = new Duo<>(entry.getKey(), identifier);
						break;
					}
				}
				result.computeIfAbsent(duo.getLeft(), k -> new HashSet<>()).add(duo.getRight());
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
	private static class SingleLoadEntityJoinTree<C, I> extends EntityJoinTree<C, I> {
		
		public <T extends Table<T>> SingleLoadEntityJoinTree(ConfiguredRelationalPersister<C, I> mainPersister,
															 Set<? extends ConfiguredRelationalPersister<C, I>> subPersisters) {
			super(tree -> new JoinTableRootJoinNode<>(
					tree,
					mainPersister,
					subPersisters,
					mainPersister.<T>getMapping().getSelectableColumns(),
					(T) mainPersister.getMainTable())
			);
		}
	}
}
