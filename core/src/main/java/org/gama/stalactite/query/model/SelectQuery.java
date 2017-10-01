package org.gama.stalactite.query.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gama.lang.reflect.MethodDispatcher;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

import static org.gama.stalactite.query.model.AbstractCriterion.LogicalOperator.And;
import static org.gama.stalactite.query.model.AbstractCriterion.LogicalOperator.Or;

/**
 * @author mary
 */
public class SelectQuery implements FromTrailer {
	
	private final FluentSelect select;
	private final FluentFrom from;
	private final From fromSurrogate;
	private final FluentWhere where;
	private final GroupBy groupBy;

	public SelectQuery() {
		this.select = new FluentSelect();
		this.fromSurrogate = new From();
		this.from = new MethodDispatcher()
				.fallbackOn(new Object())
				.redirect(JoinChain.class, fromSurrogate, true)
				.redirect(FromTrailer.class, this)
				.build(FluentFrom.class);
		this.where = new FluentWhere();
		this.groupBy = new GroupBy();
	}

	public FluentSelect getSelect() {
		return this.select;
	}

	public JoinChain getFrom() {
		return this.from;
	}
	
	public From getFromSurrogate() {
		return fromSurrogate;
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
	
	public FluentWhere where(Object ... criterion) {
		return this.where.and(criterion);
	}
	
	@Override
	public GroupBy groupBy(Column column, Column ... columns) {
		return this.groupBy.add(column, columns);
	}
	
	@Override
	public GroupBy groupBy(String column, String ... columns) {
		return this.groupBy.add(column, columns);
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
			SelectQuery.this.fromSurrogate.add(leftTable);
			return from;
		}
		
		public FluentFrom from(Table leftTable, String alias) {
			SelectQuery.this.fromSurrogate.add(leftTable, alias);
			return from;
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
	
	public interface FluentFrom extends JoinChain<FluentFrom>, FromTrailer {
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
		
		public GroupBy groupBy(Column column, Column ... columns) {
			return SelectQuery.this.groupBy.add(column, columns);
		}
		
		public GroupBy groupBy(String column, String ... columns) {
			return SelectQuery.this.groupBy.add(column, columns);
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

		public GroupBy add(Column column, Column ... columns) {
			add(column);
			for (Column col : columns) {
				add(col);
			}
			return this;
		}
		
		public GroupBy add(String column, String ... columns) {
			add(column);
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
