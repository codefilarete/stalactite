package org.stalactite.query.model;

import java.util.*;

import org.stalactite.lang.Strings;
import org.stalactite.lang.bean.Objects;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.stalactite.query.model.From.AbstractJoin;

/**
 * @author Guillaume Mary
 */
public class From implements Iterable<AbstractJoin> {
	
	private final List<AbstractJoin> joins = new ArrayList<>();
	
	private final Map<Table, String> tableAliases = new HashMap<>(4);

	public From() {
	}

	public List<AbstractJoin> getJoins() {
		return joins;
	}

	public Map<Table, String> getTableAliases() {
		return tableAliases;
	}

	public From innerJoin(Column leftColumn, Column rightColumn) {
		return addNewJoin(leftColumn, null, rightColumn, null, null);
	}

	public From innerJoin(Column leftColumn, String leftTableAlias, Column rightColumn, String rightTableAlias) {
		return addNewJoin(leftColumn, leftTableAlias, rightColumn, rightTableAlias, null);
	}

	public From leftOuterJoin(Column leftColumn, Column rightColumn) {
		return addNewJoin(leftColumn, null, rightColumn, null, false);
	}

	public From leftOuterJoin(Column leftColumn, String leftTableAlias, Column rightColumn, String rightTableAlias) {
		return addNewJoin(leftColumn, leftTableAlias, rightColumn, rightTableAlias, false);
	}

	public From rightOuterJoin(Column leftColumn, Column rightColumn) {
		return addNewJoin(leftColumn, null, rightColumn, null, true);
	}

	public From rightOuterJoin(Column leftColumn, String leftTableAlias, Column rightColumn, String rightTableAlias) {
		return addNewJoin(leftColumn, leftTableAlias, rightColumn, rightTableAlias, true);
	}

	private From addNewJoin(Column leftColumn, String leftTableAlias, Column rightColumn, String rightTableAlias, Boolean joinDirection) {
		return add(new ColumnJoin(leftColumn, leftTableAlias, rightColumn, rightTableAlias, joinDirection));
	}

	public From innerJoin(Table leftTable, Table rightTable, String joinClause) {
		return addNewJoin(leftTable, null, rightTable, null, joinClause, null);
	}

	public From innerJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause) {
		return addNewJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, null);
	}

	public From leftOuterJoin(Table leftTable, Table rigTable, String joinClause) {
		return addNewJoin(leftTable, null, rigTable, null, joinClause, false);
	}

	public From leftOuterJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause) {
		return addNewJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, false);
	}

	public From rightOuterJoin(Table leftTable, Table rigTable, String joinClause) {
		return addNewJoin(leftTable, null, rigTable, null, joinClause, true);
	}

	public From rightOuterJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause) {
		return addNewJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, true);
	}
	
	public CrossJoin add(Table table) {
		CrossJoin crossJoin = new CrossJoin(table);
		add(crossJoin);
		return crossJoin;
	}
	
	public CrossJoin add(Table table, String tableAlias) {
		CrossJoin crossJoin = new CrossJoin(table, tableAlias);
		add(crossJoin);
		return crossJoin;
	}

	private From addNewJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause, Boolean joinDirection) {
		return add(new RawTableJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, joinDirection));
	}

	From add(AbstractJoin o) {
		this.joins.add(o);
		return this;
	}

	private void addAlias(Table table, String alias) {
		if (!Strings.isEmpty(alias)) {
			this.tableAliases.put(table, alias);
		}
	}

	@Override
	public Iterator<AbstractJoin> iterator() {
		return this.joins.iterator();
	}

	public class AliasedTable extends Aliased {
		
		private final Table table;

		public AliasedTable(Table table) {
			this.table = table;
		}

		public AliasedTable(Table table, String alias) {
			super(alias);
			this.table = table;
			addAlias(table, alias);
		}
		
		public Table getTable() {
			return table;
		}

		/**
		 * Mis en place car nécessaire à {@link java.util.TreeSet#removeAll(Collection)} 
		 * (besoin au calcul de {@link org.stalactite.query.builder.FromBuilder#diff(Join, Join)})
		 * @param obj
		 * @return true si obj est une AliasedTable identique sur la Table et l'alias
		 */
		@Override
		public boolean equals(Object obj) {
			return obj instanceof AliasedTable
					&& getTable().equals(((AliasedTable) obj).getTable()) && Objects.equalsWithNull(getAlias(), ((AliasedTable) obj).getAlias());
		}
	}
	
	public abstract static class AbstractJoin {
		
		public abstract AliasedTable getLeftTable();
	}
	
	public abstract static class Join extends AbstractJoin {
		/** null = inner, false = left, true = right */
		protected final Boolean joinDirection;

		protected Join(Boolean joinDirection) {
			this.joinDirection = joinDirection;
		}

		public Boolean getJoinDirection() {
			return this.joinDirection;
		}
		
		public boolean isInner() {
			return joinDirection == null;
		}

		public boolean isLeftOuter() {
			return joinDirection != null && !joinDirection;
		}
		
		public boolean isRightOuter() {
			return joinDirection != null && joinDirection;
		}
		
		public abstract AliasedTable getRightTable();
	}
	
	public class CrossJoin extends AbstractJoin {
		
		private final AliasedTable leftTable;
		
		public CrossJoin(Table leftTable) {
			this(new AliasedTable(leftTable));
		}
		
		public CrossJoin(Table table, String tableAlias) {
			this(new AliasedTable(table, tableAlias));
		}
		
		public CrossJoin(AliasedTable leftTable) {
			this.leftTable = leftTable;
		}
		
		@Override
		public AliasedTable getLeftTable() {
			return this.leftTable;
		}
		
		public CrossJoin crossJoin(Table table) {
			CrossJoin crossJoin = new CrossJoin(table);
			From.this.add(crossJoin);
			return crossJoin;
		}
		
		public CrossJoin crossJoin(Table table, String alias) {
			CrossJoin crossJoin = new CrossJoin(table, alias);
			From.this.add(crossJoin);
			return crossJoin;
		}
		
		public RawTableJoin innerJoin(Table table, String condition) {
			return replaceThisByJoin(table, condition, null);
		}
		
		public RawTableJoin leftOuterJoin(Table table, String condition) {
			return replaceThisByJoin(table, condition, false);
		}
		
		public RawTableJoin rightOuterJoin(Table table, String condition) {
			return replaceThisByJoin(table, condition, true);
		}
		
		private RawTableJoin replaceThisByJoin(Table table, String condition, Boolean joinDirection) {
			int thisIndex = From.this.joins.indexOf(this);
			RawTableJoin newJoin = new RawTableJoin(getLeftTable().getTable(), getLeftTable().getAlias(), table, null, condition, joinDirection);
			From.this.joins.set(thisIndex, newJoin);
			return newJoin;
		}
	}
	
	public class RawTableJoin extends Join {
		private final AliasedTable leftTable;
		private final AliasedTable rightTable;
		private final String joinClause;

		public RawTableJoin(Table leftTable, Table rightTable, String joinClause, Boolean joinDirection) {
			super(joinDirection);
			this.leftTable = new AliasedTable(leftTable);
			this.joinClause = joinClause;
			this.rightTable = new AliasedTable(rightTable);
		}
		
		public RawTableJoin(Table leftTable, String leftTableAlias, Table rightTable, String rightTableAlias, String joinClause, Boolean joinDirection) {
			super(joinDirection);
			this.leftTable = new AliasedTable(leftTable, leftTableAlias);
			this.rightTable = new AliasedTable(rightTable, rightTableAlias);
			this.joinClause = joinClause;
		}
		
		public AliasedTable getLeftTable() {
			return leftTable;
		}

		public AliasedTable getRightTable() {
			return rightTable;
		}

		public String getJoinClause() {
			return joinClause;
		}
	}

	public class ColumnJoin extends Join {
		
		private final Column leftColumn;
		private final Column rightColumn;
		private final AliasedTable leftTable;
		private final AliasedTable rightTable;
		
		public ColumnJoin(Column leftColumn, Column rightColumn, Boolean joinDirection) {
			this(leftColumn, null, rightColumn, null, joinDirection);
		}

		public ColumnJoin(Column leftColumn, String leftAlias, Column rightColumn, String rightAlias, Boolean joinDirection) {
			super(joinDirection);
			this.leftColumn = leftColumn;
			this.rightColumn = rightColumn;
			this.leftTable = new AliasedTable(leftColumn.getTable(), leftAlias);
			this.rightTable = new AliasedTable(rightColumn.getTable(), rightAlias);
		}

		public Column getLeftColumn() {
			return leftColumn;
		}

		public Column getRightColumn() {
			return rightColumn;
		}

		public AliasedTable getLeftTable() {
			return leftTable;
		}

		public AliasedTable getRightTable() {
			return rightTable;
		}
	}
}
