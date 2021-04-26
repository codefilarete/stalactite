package org.gama.stalactite.persistence.engine.runtime.load;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.gama.lang.StringAppender;
import org.gama.lang.Strings;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.persistence.engine.runtime.load.AbstractJoinNode.JoinNodeHierarchyIterator;
import org.gama.stalactite.persistence.engine.runtime.load.EntityTreeInflater.ConsumerNode;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.IdentityMap;
import org.gama.stalactite.query.model.From;
import org.gama.stalactite.query.model.Query;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.gama.stalactite.sql.binder.ParameterBinderProvider;

/**
 * Builder of a {@link Query} from an {@link EntityJoinTree}
 * 
 * @author Guillaume Mary
 */
public class EntityTreeQueryBuilder<C> {
	
	private final EntityJoinTree<C, Object> tree;
	private final ParameterBinderProvider<Column> parameterBinderProvider;
	
	private final AliasBuilder aliasBuilder = new AliasBuilder();
	
	/**
	 * @param parameterBinderProvider  Will give the {@link ParameterBinder} for the reading of the final select clause
	 */
	public EntityTreeQueryBuilder(EntityJoinTree<C, ?> tree, ParameterBinderProvider<Column> parameterBinderProvider) {
		this.tree = (EntityJoinTree<C, Object>) tree;
		this.parameterBinderProvider = parameterBinderProvider;
	}
	
	
	public EntityTreeQuery<C> buildSelectQuery() {
		Query query = new Query();
		
		Map<String, ParameterBinder> selectParameterBinders = new HashMap<>();
		
		// Made IdentityMap to support presence of same table multiple times in query, in particular for cycling bean graph (tables are cloned)
		IdentityMap<Column, String> columnAliases = new IdentityMap<>();
		
		// Table clones storage per their initial node to manage several occurrence of same table in query
		Map<JoinNode, Table> tablePerJoinNode = new HashMap<>();
		
		// Simple class that helps to add columns to select, made as internal method class else it would take more parameters
		class ResultHelper {
			
			private <T1 extends Table<T1>> void addColumnsToSelect(JoinNode joinNode, String tableAlias) {
				Iterable<Column<T1, Object>> selectableColumns = joinNode.getColumnsToSelect();
				for (Column selectableColumn : selectableColumns) {
					String alias = aliasBuilder.buildColumnAlias(tableAlias, selectableColumn);
					Column columnClone = tablePerJoinNode.get(joinNode).getColumn(selectableColumn.getName());
					query.select(columnClone, alias);
					// we link the column alias to the binder so it will be easy to read the ResultSet
					selectParameterBinders.put(alias, parameterBinderProvider.getBinder(selectableColumn));
					columnAliases.put(columnClone, alias);
				}
			}
			
			/**
			 * Builds {@link ConsumerNode}s for the generated 
			 * @return
			 */
			private ConsumerNode buildConsumerTree() {
				ConsumerNode consumerRoot = new ConsumerNode(tree.getRoot().toConsumer(createDedicatedRowDecoder(tree.getRoot())));
				tree.foreachJoinWithDepth(consumerRoot, (targetOwner, currentNode) -> {
					ConsumerNode consumerNode = new ConsumerNode(currentNode.toConsumer(createDedicatedRowDecoder(currentNode)));
					targetOwner.addConsumer(consumerNode);
					return consumerNode;
				});
				return consumerRoot;
			}
			
			/**
			 * Creates a {@link ColumnedRow} dedicated to given node : because we created table clones
			 * @param node
			 * @return
			 */
			private ColumnedRow createDedicatedRowDecoder(JoinNode node) {
				return new ColumnedRow(column -> columnAliases.get(tablePerJoinNode.get(node).getColumn(column.getName())));
			}
		}
		
		ResultHelper resultHelper = new ResultHelper();
		
		/* In the following algorithm, node tables will be cloned and applied a unique alias to manage presence of twice the same table in different
		 * nodes. This happens when tree contains sibling relations (like person->firstHouse and person->secondaryHouse), or, in a more general way,
		 * maps some entities onto same table. So by cloning tables and using IdentityMap<Column, String> for alias storage we can affect different
		 * aliases to same initial table of different nodes : final alias computation can be seen at ResultHelper.createDedicatedRowDecoder(..) 
		 * Those clones doesn't affect SQL generation since table and column clones have same name as the original.
		 */
		
		// initialization of the from clause with the very first table
		JoinRoot<C, Object, ?> joinRoot = this.tree.getRoot();
		Table rootTableClone = cloneTable(joinRoot);
		tablePerJoinNode.put(joinRoot, rootTableClone);
		From from = query.getFromSurrogate().add(rootTableClone);
		resultHelper.addColumnsToSelect(joinRoot, aliasBuilder.buildTableAlias(joinRoot));
		
		// completing from clause
		this.tree.foreachJoin(join -> {
			Table copyTable = tablePerJoinNode.computeIfAbsent(join, this::cloneTable);
			String tableAlias = aliasBuilder.buildTableAlias(join);
			resultHelper.addColumnsToSelect(join, tableAlias);
			Column leftJoinColumn = tablePerJoinNode.get(join.getParent()).getColumn(join.getLeftJoinColumn().getName());
			
			Column rightJoinColumn = copyTable.getColumn(join.getRightJoinColumn().getName());
			switch (join.getJoinType()) {
				case INNER:
					from.innerJoin(leftJoinColumn, rightJoinColumn);
					break;
				case OUTER:
					from.leftOuterJoin(leftJoinColumn, rightJoinColumn);
					break;
			}
			
			from.setAlias(copyTable, tableAlias);
		});
		
		EntityTreeInflater<C> entityTreeInflater = new EntityTreeInflater<>(resultHelper.buildConsumerTree(), columnAliases,
				Maps.innerJoinOnValuesAndKeys(tree.getJoinIndex(), tablePerJoinNode));
		
		return new EntityTreeQuery<>(query, selectParameterBinders, columnAliases, entityTreeInflater);
	}
	
	/**
	 * Clones table of given join (only on its columns, no need for its foreign key clones nor indexes)
	 * 
	 * @param joinNode the join which table must be cloned
	 * @return a copy (on name and columns) of given join table
	 */
	@VisibleForTesting
	Table cloneTable(JoinNode joinNode) {
		Table table = new Table(joinNode.getTable().getName());
		((Set<Column>) joinNode.getTable().getColumns()).forEach(column ->  table.addColumn(column.getName(), column.getJavaType(), column.getSize()));
		return table;
	}
	
	/**
	 * Wrapper of {@link #buildSelectQuery()} result
	 * 
	 * @param <C>
	 */
	public static class EntityTreeQuery<C> {
		
		private final Query query;
		
		/** Mappig between column name in select and their {@link ParameterBinder} for reading */
		private final Map<String, ParameterBinder> selectParameterBinders;
		
		/**
		 * Column aliases, made as a {@link IdentityMap} to handle {@link Column} clones presence in it
		 */
		private final IdentityMap<Column, String> columnAliases;
		
		private final EntityTreeInflater<C> entityTreeInflater;
		
		private EntityTreeQuery(Query query,
								Map<String, ParameterBinder> selectParameterBinders,
								IdentityMap<Column, String> columnAliases,
								EntityTreeInflater<C> entityTreeInflater) {
			this.selectParameterBinders = selectParameterBinders;
			this.query = query;
			this.columnAliases = columnAliases;
			this.entityTreeInflater = entityTreeInflater;
		}
		
		public Query getQuery() {
			return query;
		}
		
		public Map<String, ParameterBinder> getSelectParameterBinders() {
			return selectParameterBinders;
		}
		
		public IdentityMap<Column, String> getColumnAliases() {
			return columnAliases;
		}
		
		public EntityTreeInflater<C> getInflater() {
			return entityTreeInflater;
		}
	}
	
	private static class AliasBuilder {
		
		/**
		 * Gives alias of given table root node
		 * @param joinRoot the node which {@link Table} alias must be built
		 * @return the given alias in priority or the name of the table
		 */
		public String buildTableAlias(JoinRoot joinRoot) {
			return giveTableAlias(joinRoot);
		}
		
		/**
		 * Gives alias of given table root node
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
		 * Gives the alias of a Column
		 * @param tableAlias a non-null table alias
		 * @param selectableColumn the {@link Column} for which an alias is requested
		 * @return tableAlias + "_" + column.getName()
		 */
		public String buildColumnAlias(@Nonnull String tableAlias, Column selectableColumn) {
			return tableAlias + "_" + selectableColumn.getName();
		}
	}
	
}
