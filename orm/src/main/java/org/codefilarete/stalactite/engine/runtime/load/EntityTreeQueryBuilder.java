package org.codefilarete.stalactite.engine.runtime.load;

import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.load.AbstractJoinNode.JoinNodeHierarchyIterator;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.ConsumerNode;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.query.model.From;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Union.PseudoColumn;
import org.codefilarete.stalactite.query.model.Union.UnionInFrom;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReaderRegistry;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.IdentityMap;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;

/**
 * Builder of a {@link Query} from an {@link EntityJoinTree}
 * 
 * @author Guillaume Mary
 */
public class EntityTreeQueryBuilder<C> {
	
	private final EntityJoinTree<C, Object> tree;
	private final ResultSetReaderRegistry parameterBinderProvider;
	
	private final AliasBuilder aliasBuilder = new AliasBuilder();
	
	/**
	 * @param parameterBinderProvider Will give the {@link ParameterBinder} for the reading of the final select clause
	 */
	public EntityTreeQueryBuilder(EntityJoinTree<C, ?> tree, ResultSetReaderRegistry parameterBinderProvider) {
		this.tree = (EntityJoinTree<C, Object>) tree;
		this.parameterBinderProvider = parameterBinderProvider;
	}
	
	
	public EntityTreeQuery<C> buildSelectQuery() {
		Query query = new Query();
		
		Map<String, ParameterBinder> selectParameterBinders = new HashMap<>();
		
		// Made IdentityMap to support presence of same table multiple times in query, in particular for cycling bean graph (tables are cloned)
		IdentityMap<Selectable, String> columnAliases = new IdentityMap<>();
		
		// Mapping between original Column of table in joins and Column of cloned tables. Not perfect but made to solve
		// issue with entity graph load with criteria (EntityPersister.selectWhere(..)) containing Columns
		// (ColumnCriterion) because they come from user which is one of source table, hence table alias are not found
		// for them, which causes issue while setting criteria value (column is not found by database driver)
		IdentityHashMap<Selectable<?>, Selectable<?>> columnClones = new IdentityHashMap<>();
		
		// Table clones storage per their initial node to manage several occurrence of same table in query
		Map<JoinNode, Fromable> tableClonePerJoinNode = new HashMap<>();
		
		ResultHelper resultHelper = new ResultHelper(query, parameterBinderProvider, aliasBuilder, selectParameterBinders, columnAliases, tableClonePerJoinNode);
		
		/* In the following algorithm, node tables will be cloned and applied a unique alias to manage presence of twice the same table in different
		 * nodes. This happens when tree contains sibling relations (like person->firstHouse and person->secondaryHouse), or, in a more general way,
		 * maps some entities onto same table. So by cloning tables and using IdentityMap<Column, String> for alias storage we can affect different
		 * aliases to same initial table of different nodes : final alias computation can be seen at ResultHelper.createDedicatedRowDecoder(..) 
		 * Those clones don't affect SQL generation since table and column clones have same name as the original.
		 */
		
		// initialization of the From clause with the very first table
		JoinRoot<C, Object, ?> joinRoot = this.tree.getRoot();
		Duo<Fromable, IdentityHashMap<Selectable<?>, Selectable<?>>> rootClone = cloneTable(joinRoot);
		Fromable rootTableClone = rootClone.getLeft();
		columnClones.putAll(rootClone.getRight());
		tableClonePerJoinNode.put(joinRoot, rootTableClone);
		query.getFromSurrogate().setRoot(rootTableClone);
		resultHelper.addColumnsToSelect(joinRoot, aliasBuilder.buildTableAlias(joinRoot));
		
		// completing from clause
		this.tree.foreachJoin(join -> {
			Duo<Fromable, IdentityHashMap<Selectable<?>, Selectable<?>>> joinClone = cloneTable(join);
			tableClonePerJoinNode.put(join, joinClone.getLeft());
			columnClones.putAll(joinClone.getRight());
		});
		resultHelper.applyJoinTree(this.tree);
		
		EntityTreeInflater<C> entityTreeInflater = new EntityTreeInflater<>(resultHelper.buildConsumerTree(tree),
																			columnAliases,
																			Maps.innerJoinOnValuesAndKeys(tree.getJoinIndex(), tableClonePerJoinNode));
		
		return new EntityTreeQuery<>(query, selectParameterBinders, columnAliases, entityTreeInflater, columnClones);
	}
	
	/**
	 * Clones table of given join (only on its columns, no need for its foreign key clones nor indexes)
	 * 
	 * @param joinNode the join which table must be cloned
	 * @return a copy (on name and columns) of given join table
	 */
	@VisibleForTesting
	Duo<Fromable, IdentityHashMap<Selectable<?>, Selectable<?>>> cloneTable(JoinNode joinNode) {
		Fromable joinFromable = joinNode.getTable();
		if (joinFromable instanceof Table) {
			Table table = new Table(joinFromable.getName());
			IdentityHashMap<Selectable<?>, Selectable<?>> columnClones = new IdentityHashMap<>(table.getColumns().size());
			(((Table<?>) joinFromable).getColumns()).forEach(column -> {
				Column clone = table.addColumn(column.getName(), column.getJavaType(), column.getSize());
				columnClones.put(column, clone);
			});
			return new Duo<>(table, columnClones);
		} else if (joinFromable instanceof UnionInFrom) {
			UnionInFrom unionInFrom = new UnionInFrom(joinFromable.getName(), ((UnionInFrom) joinFromable).getUnion());
			IdentityHashMap<Selectable<?>, Selectable<?>> columnClones = new IdentityHashMap<>(unionInFrom.getColumns().size());
			(((UnionInFrom) joinFromable).getColumns()).forEach(column -> {
				PseudoColumn<?> clone = unionInFrom.getUnion().registerColumn(column.getExpression(), column.getJavaType());
				columnClones.put(column, clone);
			});
			return new Duo<>(unionInFrom, columnClones);
		} else {
			throw new UnsupportedOperationException("Cloning " + Reflections.toString(joinNode.getTable().getClass()) + " is not implemented");
		}
	}
	
	// Simple class that helps to add columns to select, made as internal method class else it would take more parameters
	private static class ResultHelper {
		
		private final ResultSetReaderRegistry parameterBinderProvider;
		
		private final AliasBuilder aliasBuilder;
		
		private final Query query;
		
		private final Map<String, ResultSetReader> selectParameterBinders;
		
		// Made IdentityMap to support presence of same table multiple times in query, in particular for cycling bean graph (tables are cloned)
		private final IdentityMap<Selectable, String> columnAliases;
		
		// Table clones storage per their initial node to manage several occurrence of same table in query
		private final Map<JoinNode, Fromable> tablePerJoinNode;
		
		private ResultHelper(Query query,
							 ResultSetReaderRegistry parameterBinderProvider,
							 AliasBuilder aliasBuilder,
							 Map<String, ? extends ResultSetReader> selectParameterBinders,
							 IdentityMap<Selectable, String> columnAliases,
							 Map<JoinNode, Fromable> tablePerJoinNode) {
			this.parameterBinderProvider = parameterBinderProvider;
			this.aliasBuilder = aliasBuilder;
			this.query = query;
			this.selectParameterBinders = (Map<String, ResultSetReader>) selectParameterBinders;
			this.columnAliases = columnAliases;
			this.tablePerJoinNode = tablePerJoinNode;
		}
		
		/**
		 * Mimics given tree joins onto internal {@link Query} by using table clones (given at construction time)
		 * 
		 * @param tree join structure to be mimicked
		 * @param <JOINTYPE> internal type of join to avoid weird cast or type loss
		 */
		private <JOINTYPE> void applyJoinTree(EntityJoinTree<?, ?> tree) {
			From targetFrom = query.getFromSurrogate();
			tree.foreachJoin(join -> {
				String tableAlias = aliasBuilder.buildTableAlias(join);
				addColumnsToSelect(join, tableAlias);
				// we look for the cloned equivalent column of the original ones (from join node)
				KeyBuilder<Fromable, JOINTYPE> leftJoinLinkClone = Key.from(tablePerJoinNode.get(join.getParent()));
				join.getLeftJoinLink().getColumns().forEach(column -> {
					leftJoinLinkClone.addColumn(tablePerJoinNode.get(join.getParent()).findColumn(column.getExpression()));
				});
				Key<Fromable, JOINTYPE> leftJoinColumn = leftJoinLinkClone.build();
				
				Fromable tableClone = tablePerJoinNode.get(join);
				KeyBuilder<Fromable, JOINTYPE> rightJoinLinkClone = Key.from(tablePerJoinNode.get(join));
				join.getRightJoinLink().getColumns().forEach(column -> {
					rightJoinLinkClone.addColumn(tableClone.findColumn(column.getExpression()));
				});
				Key<Fromable, JOINTYPE> rightJoinColumn = rightJoinLinkClone.build();
				switch (join.getJoinType()) {
					case INNER:
						targetFrom.innerJoin(leftJoinColumn, rightJoinColumn);
						break;
					case OUTER:
						targetFrom.leftOuterJoin(leftJoinColumn, rightJoinColumn);
						break;
				}
				
				targetFrom.setAlias(tableClone, tableAlias);
			});
		}
		
		private <T1 extends Fromable> void addColumnsToSelect(JoinNode<T1> joinNode, String tableAlias) {
			Set<Selectable<Object>> selectableColumns = joinNode.getColumnsToSelect();
			for (Selectable<?> selectableColumn : selectableColumns) {
				String alias = aliasBuilder.buildColumnAlias(tableAlias, selectableColumn);
				Selectable columnClone = tablePerJoinNode.get(joinNode).findColumn(selectableColumn.getExpression());
				query.select(columnClone, alias);
				// we link the column alias to the binder so it will be easy to read the ResultSet
				ResultSetReader<?> reader;
				if (selectableColumn instanceof Column) {
					reader = parameterBinderProvider.getReader((Column) selectableColumn);
					selectParameterBinders.put(alias, reader);
				} else {
					reader = parameterBinderProvider.getReader(selectableColumn.getJavaType());
				}
				selectParameterBinders.put(alias, reader);
				columnAliases.put(columnClone, alias);
			}
		}
		
		/**
		 * Builds {@link ConsumerNode}s for the generated query
		 * @return the root {@link ConsumerNode} for given tree
		 */
		private ConsumerNode buildConsumerTree(EntityJoinTree<?, ?> tree) {
			ConsumerNode consumerRoot = new ConsumerNode(tree.getRoot().toConsumer(createDedicatedRowDecoder(tree.getRoot())));
			tree.foreachJoinWithDepth(consumerRoot, (targetOwner, currentNode) -> {
				JoinRowConsumer consumer = currentNode.toConsumer(createDedicatedRowDecoder(currentNode));
				ConsumerNode consumerNode = new ConsumerNode(consumer);
				targetOwner.addConsumer(consumerNode);
				return consumerNode;
			});
			return consumerRoot;
		}
		
		/**
		 * Creates a {@link ColumnedRow} dedicated to given node because we created table clones
		 * @param node the node for which we'll create a {@link ColumnedRow}
		 * @return a {@link ColumnedRow} that gives aliases according to them in {@link #columnAliases}
		 */
		private ColumnedRow createDedicatedRowDecoder(JoinNode node) {
			return new ColumnedRow(column -> columnAliases.get(tablePerJoinNode.get(node).findColumn(column.getExpression())));
		}
	}
	
	/**
	 * Wrapper of {@link #buildSelectQuery()} result
	 * 
	 * @param <C>
	 */
	public static class EntityTreeQuery<C> {
		
		private final Query query;
		
		/** Mapping between column name in select and their {@link ParameterBinder} for reading */
		private final Map<String, ParameterBinder> selectParameterBinders;
		
		/**
		 * Column aliases, made as a {@link IdentityMap} to handle {@link Column} clones presence in it
		 */
		private final IdentityMap<Selectable, String> columnAliases;
		
		private final EntityTreeInflater<C> entityTreeInflater;
		
		private final IdentityHashMap<Selectable<?>, Selectable<?>> columnClones;
		
		private EntityTreeQuery(Query query,
								Map<String, ParameterBinder> selectParameterBinders,
								IdentityMap<Selectable, String> columnAliases,
								EntityTreeInflater<C> entityTreeInflater,
								IdentityHashMap<Selectable<?>, Selectable<?>> columnClones) {
			this.selectParameterBinders = selectParameterBinders;
			this.query = query;
			this.columnAliases = columnAliases;
			this.entityTreeInflater = entityTreeInflater;
			this.columnClones = columnClones;
		}
		
		public Query getQuery() {
			return query;
		}
		
		public Map<String, ParameterBinder> getSelectParameterBinders() {
			return selectParameterBinders;
		}
		
		public IdentityMap<Selectable, String> getColumnAliases() {
			return columnAliases;
		}
		
		public EntityTreeInflater<C> getInflater() {
			return entityTreeInflater;
		}
		
		/**
		 * Gives the mapping between original {@link Column} of {@link Table} in joins and {@link Column} of cloned
		 * {@link Table}s.
		 * Made to allow external users to find the internal column (cloned one) for their original one
		 * @return the mapping between original {@link Column} of {@link Table} in joins and {@link Column} of cloned {@link Table}s.
		 */
		public IdentityHashMap<Selectable<?>, Selectable<?>> getColumnClones() {
			return columnClones;
		}
	}
	
	private static class AliasBuilder {
		
		/**
		 * Gives alias of given table root node
		 * 
		 * @param joinRoot the node which {@link Table} alias must be built
		 * @return the given alias in priority or the name of the table
		 */
		public String buildTableAlias(JoinRoot joinRoot) {
			return giveTableAlias(joinRoot);
		}
		
		/**
		 * Gives alias of given table root node
		 * 
		 * @param node the node which {@link Table} alias must be built
		 * @return node alias if present, else node table name
		 */
		private String giveTableAlias(JoinNode node) {
			return Strings.preventEmpty(node.getTableAlias(), node.getTable().getName());
		}
		
		public String buildTableAlias(AbstractJoinNode joinNode) {
			StringAppender aliasBuilder = new StringAppender() {
				@Override
				public StringAppender cat(Object o) {
					if (o instanceof AbstractJoinNode) {
						AbstractJoinNode<?, ?, ?, ?> localNode = (AbstractJoinNode<?, ?, ?, ?>) o;
						return super.cat(giveTableAlias(localNode));
					}
					return super.cat(o);
				}
			};
			List<AbstractJoinNode> nodeParents = Iterables.copy(new JoinNodeHierarchyIterator(joinNode));
			// we reverse parent list for clarity while looking at SQL, not mandatory since it already makes a unique path
			Collections.reverse(nodeParents);
			aliasBuilder.ccat(nodeParents, "_");
			return aliasBuilder.toString();
		}
		
		/**
		 * Gives the alias of a {@link Column}
		 * 
		 * @param tableAlias a non-null table alias
		 * @param selectableColumn the {@link Column} for which an alias is requested
		 * @return tableAlias + "_" + column.getName()
		 */
		public String buildColumnAlias(String tableAlias, Selectable selectableColumn) {
			return tableAlias + "_" + selectableColumn.getExpression();
		}
	}
	
}
