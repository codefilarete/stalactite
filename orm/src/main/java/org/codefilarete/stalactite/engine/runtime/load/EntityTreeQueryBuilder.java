package org.codefilarete.stalactite.engine.runtime.load;

import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.load.AbstractJoinNode.JoinNodeHierarchyIterator;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.ConsumerNode;
import org.codefilarete.stalactite.query.model.From;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReaderRegistry;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.IdentityMap;
import org.codefilarete.tool.collection.Iterables;

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
		Map<Selectable<?>, ParameterBinder<?>> selectParameterBinders = new HashMap<>();
		
		ResultHelper resultHelper = new ResultHelper(query, parameterBinderProvider, aliasBuilder, selectParameterBinders);
		
		/* In the following algorithm, node tables will be cloned and applied a unique alias to manage the presence of twice the same table in different
		 * nodes. This happens when the tree contains sibling relations (like person->firstHouse and person->secondaryHouse), or, in a more general way,
		 * maps some entities onto the same table. So by cloning tables and using IdentityMap<Column, String> for alias storage we can affect different
		 * aliases to the same initial table of different nodes : final alias computation can be seen at ResultHelper.createDedicatedRowDecoder(..) 
		 * Those clones don't affect SQL generation since table and column clones have the same name as the original.
		 */
		
		// initialization of the From clause with the very first table
		JoinRoot<C, Object, ?> joinRoot = this.tree.getRoot();
		query.getFromDelegate().setRoot(joinRoot.getTable());
		resultHelper.addColumnsToSelectClause(joinRoot, aliasBuilder.buildTableAlias(joinRoot));
		
		// completing from clause
		resultHelper.applyJoinTree(this.tree);
		
		EntityTreeInflater<C> entityTreeInflater = new EntityTreeInflater<>(resultHelper.buildConsumerTree(tree));
		
		return new EntityTreeQuery<>(query, selectParameterBinders, entityTreeInflater);
	}
	
	/**
	 * Clones table of given join (only on its columns, no need for its foreign key clones nor indexes)
	 * 
	 * @param joinNode the join which table must be cloned
	 * @return a copy (on name and columns) of given join table
	 */
	@VisibleForTesting
	Duo<Fromable, IdentityHashMap<? extends Selectable<?>, ? extends Selectable<?>>> cloneTable(JoinNode joinNode) {
		return new Duo<>(joinNode.getTable(), joinNode.getOriginalColumnsToLocalOnes());
	}
	
	// Simple class that helps to add columns to select
	private static class ResultHelper {
		
		private final ResultSetReaderRegistry parameterBinderProvider;
		
		private final AliasBuilder aliasBuilder;
		
		private final Query query;
		
		private final Map<Selectable<?>, ResultSetReader<?>> selectParameterBinders;
		
		// Made IdentityMap to support presence of same table multiple times in query, in particular for cycling bean graph (tables are cloned)
		private final IdentityMap<Selectable<?>, String> columnAliases = new IdentityMap<>();
		
		private ResultHelper(Query query,
							 ResultSetReaderRegistry parameterBinderProvider,
							 AliasBuilder aliasBuilder,
							 Map<? extends Selectable<?>, ? extends ResultSetReader<?>> selectParameterBinders) {
			this.parameterBinderProvider = parameterBinderProvider;
			this.aliasBuilder = aliasBuilder;
			this.query = query;
			this.selectParameterBinders = (Map<Selectable<?>, ResultSetReader<?>>) selectParameterBinders;
		}
		
		/**
		 * Mimics given tree joins onto internal {@link Query} by using table clones (given at construction time)
		 * 
		 * @param tree join structure to be mimicked
		 * @param <JOINTYPE> internal type of join to avoid weird cast or type loss
		 */
		private <JOINTYPE> void applyJoinTree(EntityJoinTree<?, ?> tree) {
			From targetFrom = query.getFromDelegate();
			tree.foreachJoin(join -> {
				String tableAlias = aliasBuilder.buildTableAlias(join);
				addColumnsToSelectClause(join, tableAlias);
				// we look for the cloned equivalent column of the original ones (from join node)
				Key<Fromable, JOINTYPE> leftJoinColumn = (Key<Fromable, JOINTYPE>) join.getLeftJoinLink();
				Key<Fromable, JOINTYPE> rightJoinColumn = (Key<Fromable, JOINTYPE>) join.getRightJoinLink();
				Fromable tableClone = join.getRightTable();
				targetFrom.setAlias(tableClone, tableAlias);
				switch (join.getJoinType()) {
					case INNER:
						targetFrom.innerJoin(leftJoinColumn, rightJoinColumn);
						break;
					case OUTER:
						targetFrom.leftOuterJoin(leftJoinColumn, rightJoinColumn);
						break;
				}
			});
		}
		
		private <T1 extends Fromable> void addColumnsToSelectClause(JoinNode<?, T1> joinNode, String tableAlias) {
			Set<Selectable<?>> selectableColumns = joinNode.getColumnsToSelect();
			for (Selectable<?> selectableColumn : selectableColumns) {
				Selectable<?> columnClone = joinNode.getOriginalColumnsToLocalOnes().get(selectableColumn); 
				String alias = aliasBuilder.buildColumnAlias(tableAlias, selectableColumn);
				query.select(columnClone, alias);
				// we link the column alias to the binder so it will be easy to read the ResultSet
				ResultSetReader<?> reader;
				if (selectableColumn instanceof Column) {
					reader = parameterBinderProvider.getReader((Column) selectableColumn);
				} else {
					reader = parameterBinderProvider.getReader(selectableColumn.getJavaType());
				}
				selectParameterBinders.put(columnClone, reader);
				columnAliases.put(columnClone, alias);
			}
		}
		
		/**
		 * Builds {@link ConsumerNode}s for the generated query
		 * @return the root {@link ConsumerNode} for given tree
		 */
		private ConsumerNode buildConsumerTree(EntityJoinTree<?, ?> tree) {
			JoinNode root = tree.getRoot();
			ConsumerNode consumerRoot = new ConsumerNode(tree.getRoot().toConsumer(root));
			tree.foreachJoinWithDepth(consumerRoot, (targetOwner, currentNode) -> {
				JoinRowConsumer consumer = currentNode.toConsumer((JoinNode) currentNode);
				ConsumerNode consumerNode = new ConsumerNode(consumer);
				targetOwner.addConsumer(consumerNode);
				return consumerNode;
			});
			return consumerRoot;
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
		private final Map<Selectable<?>, ParameterBinder<?>> selectParameterBinders;
		
		private final EntityTreeInflater<C> entityTreeInflater;
		
		private EntityTreeQuery(Query query,
								Map<Selectable<?>, ParameterBinder<?>> selectParameterBinders,
								EntityTreeInflater<C> entityTreeInflater) {
			this.selectParameterBinders = selectParameterBinders;
			this.query = query;
			this.entityTreeInflater = entityTreeInflater;
		}
		
		public Query getQuery() {
			return query;
		}
		
		public Map<Selectable<?>, ParameterBinder<?>> getSelectParameterBinders() {
			return selectParameterBinders;
		}
		
		public Map<Selectable<?>, String> getColumnAliases() {
			return query.getAliases();
		}
		
		public EntityTreeInflater<C> getInflater() {
			return entityTreeInflater;
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
