package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.gama.stalactite.query.model.AbstractCriterion.LogicalOperator.And;
import static org.gama.stalactite.query.model.AbstractCriterion.LogicalOperator.Or;

/**
 * @author mary
 */
public class SelectQuery {
	
	private final FluentSelect select;
	private final FluentFrom from;
	private final FluentWhere where;
	private final GroupBy groupBy;

	public SelectQuery() {
		this.select = new FluentSelect();
		this.from = new FluentFrom();
		this.where = new FluentWhere();
		this.groupBy = new GroupBy();
	}

	public FluentSelect getSelect() {
		return this.select;
	}

	public From getFrom() {
		return this.from;
	}

	public Where getWhere() {
		return where;
	}

	public GroupBy getGroupBy() {
		return groupBy;
	}

	public FluentSelect select(String selectable) {
		return this.select.add(selectable);
	}

	public FluentSelect select(Column column) {
		return this.select.add(column);
	}
	
	public FluentSelect select(Column ... columns) {
		return this.select.add(columns);
	}
	
	public FluentSelect select(String ... columns) {
		return this.select.add(columns);
	}
	
	public FluentSelect select(Column column, String alias) {
		return this.select.add(column, alias);
	}
	
	public FluentSelect select(Map<Column, String> aliasedColumns) {
		return this.select.add(aliasedColumns);
	}
	
	public FluentFrom from(Column leftColumn, Column rightColumn) {
		return this.from.innerJoin(leftColumn, rightColumn);
	}

	public FluentFrom from(Column leftColumn, String leftColumnAlias, Column rightColumn, String rightColumnAlias) {
		return this.from.innerJoin(leftColumn, leftColumnAlias, rightColumn, rightColumnAlias);
	}
	
	public FluentFrom from(Table leftTable, Table rightTable, String joinCondition) {
		return this.from.innerJoin(leftTable, rightTable, joinCondition);
	}
	
	public FluentFrom from(Table leftTable, String tableAlias) {
		return this.from.crossJoin(leftTable, tableAlias);
	}
	
	public FluentFrom from(Table leftTable, String leftTableAlias, Table rightTable, String rightTableAlias, String joinCondition) {
		return this.from.innerJoin(leftTable, leftTableAlias, rightTable, rightTableAlias, joinCondition);
	}

	public FluentWhere where(Column column, CharSequence condition) {
		return this.where.and(column, condition);
	}

	public FluentWhere where(Criteria criteria) {
		return this.where.and(criteria);
	}
	
	public GroupBy groupBy(Column column) {
		return this.groupBy.add(column);
	}
	
	public GroupBy groupBy(Column ... columns) {
		return this.groupBy.add(columns);
	}
	
	public GroupBy groupBy(String ... columns) {
		return this.groupBy.add(columns);
	}

	public class FluentSelect extends Select {
				
		public FluentSelect add(Column column) {
			super.add(column);
			return this;
		}
		
		public FluentSelect add(Column ... columns) {
			super.add(columns);
			return this;
		}
		
		public FluentSelect add(String ... columns) {
			super.add(columns);
			return this;
		}
		
		public FluentSelect add(Column column, String alias) {
			super.add(column, alias);
			return this;
		}
		
		public FluentSelect add(Map<Column, String> aliasedColumns) {
			super.add(aliasedColumns);
			return this;
		}
		
		public FluentFrom from(Table leftTable) {
			return SelectQuery.this.from.from(leftTable);
		}
		
		public FluentFrom from(Table leftTable, String alias) {
			return SelectQuery.this.from.from(leftTable, alias);
		}
		
		public FluentFrom from(Table leftTable, Table rightTable, String joinCondition) {
			return SelectQuery.this.from.innerJoin(leftTable, rightTable, joinCondition);
		}
		
		public FluentFrom from(Table leftTable, String leftTableAlias, Table rightTable, String rightTableAlias, String joinCondition) {
			return SelectQuery.this.from.innerJoin(leftTable, leftTableAlias, rightTable, rightTableAlias, joinCondition);
		}
		
		public FluentFrom from(Column leftColumn, Column rightColumn) {
			return SelectQuery.this.from.innerJoin(leftColumn, rightColumn);
		}
		
		public FluentFrom from(Column leftColumn, String leftColumnAlias, Column rightColumn, String rightColumnAlias) {
			return SelectQuery.this.from.innerJoin(leftColumn, leftColumnAlias, rightColumn, rightColumnAlias);
		}
		
		public FluentFrom fromLeftOuter(Column leftColumn, Column rightColumn) {
			return SelectQuery.this.from.leftOuterJoin(leftColumn, rightColumn);
		}
		
		public FluentFrom fromLeftOuter(Column leftColumn, String leftColumnAlias, Column rightColumn, String rightColumnAlias) {
			return SelectQuery.this.from.leftOuterJoin(leftColumn, leftColumnAlias, rightColumn, rightColumnAlias);
		}
		
		public FluentFrom fromRightOuter(Column leftColumn, Column rightColumn) {
			return SelectQuery.this.from.rightOuterJoin(leftColumn, rightColumn);
		}
		
		public FluentFrom fromRightOuter(Column leftColumn, String leftColumnAlias, Column rightColumn, String rightColumnAlias) {
			return SelectQuery.this.from.rightOuterJoin(leftColumn, leftColumnAlias, rightColumn, rightColumnAlias);
		}
		
	}
	
	public class FluentFrom extends From {
		public FluentFrom from(Table leftTable) {
			super.add(leftTable);
			return this;
		}

		public FluentFrom from(Table leftTable, String alias) {
			super.add(leftTable, alias);
			return this;
		}
		
		public FluentFrom crossJoin(Table table) {
			super.crossJoin(table);
			return this;
		}
		
		public FluentFrom crossJoin(Table table, String tableAlias) {
			super.crossJoin(table, tableAlias);
			return this;
		}
		
		public FluentFrom innerJoin(Column leftColumn, Column rightColumn) {
			super.innerJoin(leftColumn, rightColumn);
			return this;
		}

		public FluentFrom innerJoin(Column leftColumn, String leftTableAlias, Column rightColumn, String rightTableAlias) {
			super.innerJoin(leftColumn, leftTableAlias, rightColumn, rightTableAlias);
			return this;
		}

		public FluentFrom leftOuterJoin(Column leftColumn, Column rightColumn) {
			super.leftOuterJoin(leftColumn, rightColumn);
			return this;
		}

		public FluentFrom leftOuterJoin(Column leftColumn, String leftTableAlias, Column rightColumn, String rightTableAlias) {
			super.leftOuterJoin(leftColumn, leftTableAlias, rightColumn, rightTableAlias);
			return this;
		}

		public FluentFrom rightOuterJoin(Column leftColumn, Column rightColumn) {
			super.rightOuterJoin(leftColumn, rightColumn);
			return this;
		}

		public FluentFrom rightOuterJoin(Column leftColumn, String leftTableAlias, Column rightColumn, String rightTableAlias) {
			super.rightOuterJoin(leftColumn, leftTableAlias, rightColumn, rightTableAlias);
			return this;
		}

		public FluentFrom innerJoin(Table leftTable, Table rightTable, String joinClause) {
			super.innerJoin(leftTable, rightTable, joinClause);
			return this;
		}

		public FluentFrom innerJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause) {
			super.innerJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause);
			return this;
		}

		public FluentFrom leftOuterJoin(Table leftTable, Table rigTable, String joinClause) {
			super.leftOuterJoin(leftTable, rigTable, joinClause);
			return this;
		}

		public FluentFrom leftOuterJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause) {
			super.leftOuterJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause);
			return this;
		}

		public FluentFrom rightOuterJoin(Table leftTable, Table rigTable, String joinClause) {
			super.rightOuterJoin(leftTable, rigTable, joinClause);
			return this;
		}

		public FluentFrom rightOuterJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause) {
			super.rightOuterJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause);
			return this;
		}
		
		public FluentWhere where(Column column, String condition) {
			return SelectQuery.this.where.and(column, condition);
		}
		
		public FluentWhere where(Criteria criteria) {
			return SelectQuery.this.where.and(criteria);
		}
		
		public FluentWhere where(Object ... criterion) {
			return SelectQuery.this.where.and(criterion);
		}
		
		public GroupBy groupBy(Column column) {
			return SelectQuery.this.groupBy.add(column);
		}
		
		public GroupBy groupBy(Column ... columns) {
			return SelectQuery.this.groupBy.add(columns);
		}
		
		public GroupBy groupBy(String ... columns) {
			return SelectQuery.this.groupBy.add(columns);
		}
	}
	
	public class FluentWhere extends Where<FluentWhere> {
		
		@Override
		public FluentWhere and(Column column, CharSequence condition) {
			return super.and(column, condition);
		}
	
		@Override
		public FluentWhere or(Column column, CharSequence condition) {
			return super.or(column, condition);
		}
	
		@Override
		public FluentWhere and(Criteria criteria) {
			return super.and(criteria);
		}
	
		@Override
		public FluentWhere or(Criteria criteria) {
			return super.or(criteria);
		}
		
		@Override
		public FluentWhere and(Object... columns) {
			return add(new RawCriterion(And, columns));
		}
		
		@Override
		public FluentWhere or(Object... columns) {
			return add(new RawCriterion(Or, columns));
		}
		
		public GroupBy groupBy(Column column) {
			return SelectQuery.this.groupBy.add(column);
		}
		
		public GroupBy groupBy(Column ... columns) {
			return SelectQuery.this.groupBy.add(columns);
		}
		
		public GroupBy groupBy(String ... columns) {
			return SelectQuery.this.groupBy.add(columns);
		}
	}

	public static class GroupBy {
		/** Column, String */
		private final List<Object> groups = new ArrayList<>();
		private final Having having = new Having();

		private GroupBy add(Object table) {
			this.groups.add(table);
			return this;
		}

		public List<Object> getGroups() {
			return groups;
		}

		public Having getHaving() {
			return having;
		}

		public GroupBy add(Column column) {
			return add((Object) column);
		}
		
		public GroupBy add(Column ... columns) {
			for (Column col : columns) {
				add(col);
			}
			return this;
		}
		
		public GroupBy add(String ... columns) {
			for (String col : columns) {
				add(col);
			}
			return this;
		}
		
		public Having having(Column column, String condition) {
			return this.having.and(column, condition);
		}
		
		public Having having(Object ... columns) {
			return this.having.and(columns);
		}
	}
	
	public static class Having extends Criteria<Having> {
		
		@Override
		public Having and(Column column, CharSequence condition) {
			return super.and(column, condition);
		}
	
		@Override
		public Having or(Column column, CharSequence condition) {
			return super.or(column, condition);
		}
	
		@Override
		public Having and(Criteria criteria) {
			return super.and(criteria);
		}
	
		@Override
		public Having or(Criteria criteria) {
			return super.or(criteria);
		}
		
		@Override
		public Having and(Object... columns) {
			return add(new RawCriterion(And, columns));
		}
		
		@Override
		public Having or(Object... columns) {
			return add(new RawCriterion(Or, columns));
		}
	}
}
