package org.gama.stalactite.persistence.engine.runtime.load;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

import org.gama.lang.Strings;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.From;
import org.gama.stalactite.query.model.Query;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.gama.stalactite.sql.binder.ParameterBinderProvider;

import static org.gama.stalactite.query.model.From.AbstractJoin.JoinDirection.INNER_JOIN;
import static org.gama.stalactite.query.model.From.AbstractJoin.JoinDirection.LEFT_OUTER_JOIN;

/**
 * Builder of a {@link Query} from an {@link EntityJoinTree}
 * 
 * @author Guillaume Mary
 */
public class EntityTreeQueryBuilder<C> {
	
	private final EntityJoinTree<C, Object> tree;
	
	private final AliasBuilder aliasBuilder = new AliasBuilder();
	
	public EntityTreeQueryBuilder(EntityJoinTree<C, ?> tree) {
		this.tree = (EntityJoinTree<C, Object>) tree;
	}
	
	/** 
	 * @param parameterBinderProvider  Will give the {@link ParameterBinder} for the reading of the final select clause
	 */
	public EntityTreeQuery<C> buildSelectQuery(ParameterBinderProvider<Column> parameterBinderProvider) {
		Query query = new Query();
		
		Map<String, ParameterBinder> selectParameterBinders = new HashMap<>();
		
		Map<Column, String> columnAliases = new HashMap<>();
		
		// Simple class that helps to add columns to select, made as internal method class else it would take more parameters
		class ResultHelper {
			
			private <T1 extends Table<T1>> void addColumnsToSelect(String tableAliasOverride, Iterable<Column<T1, Object>> selectableColumns) {
				for (Column selectableColumn : selectableColumns) {
					String tableAlias = aliasBuilder.buildTableAlias(selectableColumn.getTable(), tableAliasOverride);
					String alias = aliasBuilder.buildColumnAlias(tableAlias, selectableColumn);
					query.select(selectableColumn, alias);
					// we link the column alias to the binder so it will be easy to read the ResultSet
					selectParameterBinders.put(alias, parameterBinderProvider.getBinder(selectableColumn));
					columnAliases.put(selectableColumn, alias);
				}
			}
		}
		
		ResultHelper resultHelper = new ResultHelper();
		
		// initialization of the from clause with the very first table
		JoinRoot<C, Object, ?> joinRoot = this.tree.getRoot();
		From from = query.getFromSurrogate().add(joinRoot.getTable());
		resultHelper.addColumnsToSelect(joinRoot.getTableAlias(), joinRoot.getEntityInflater().getSelectableColumns());
		
		this.tree.foreachJoin(join -> {
			resultHelper.addColumnsToSelect(join.getTableAlias(), join.getColumnsToSelect());
			Column leftJoinColumn = join.getLeftJoinColumn();
			Column rightJoinColumn = join.getRightJoinColumn();
			from.add(from.new ColumnJoin(leftJoinColumn, rightJoinColumn, join.getJoinType() == JoinType.OUTER ? LEFT_OUTER_JOIN : INNER_JOIN));
		});
		
		return new EntityTreeQuery<>(query, this.tree, selectParameterBinders, columnAliases);
	}
	
	/**
	 * Wrapper of {@link #buildSelectQuery(ParameterBinderProvider)} result
	 * 
	 * @param <C>
	 */
	public static class EntityTreeQuery<C> {
		
		private final Query query;
		
		private final EntityJoinTree<C, Object> tree;
		
		/** Mappig between column name in select and their {@link ParameterBinder} for reading */
		private final Map<String, ParameterBinder> selectParameterBinders;
		
		private final Map<Column, String> columnAliases;
		
		private EntityTreeQuery(Query query,
								EntityJoinTree<C, ?> tree,
								Map<String, ParameterBinder> selectParameterBinders,
								Map<Column, String> columnAliases) {
			this.tree = (EntityJoinTree<C, Object>) tree;
			this.selectParameterBinders = selectParameterBinders;
			this.query = query;
			this.columnAliases = columnAliases;
		}
		
		public Query getQuery() {
			return query;
		}
		
		public Map<String, ParameterBinder> getSelectParameterBinders() {
			return selectParameterBinders;
		}
		
		public Map<Column, String> getColumnAliases() {
			return columnAliases;
		}
		
		public EntityTreeInflater<C> toInflater() {
			return new EntityTreeInflater<>(tree, new ColumnedRow(columnAliases::get));
		}
	}
	
	private static class AliasBuilder {
		
		/**
		 * Gives the alias of a table
		 * @param table the {@link Table} for which an alias is requested
		 * @param aliasOverride an optional given alias
		 * @return the given alias in priority or the name of the table
		 */
		public String buildTableAlias(Table table, String aliasOverride) {
			return (String) Strings.preventEmpty(aliasOverride, table.getName());
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
