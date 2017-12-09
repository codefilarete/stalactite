package org.gama.stalactite.query.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gama.lang.Strings;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.model.From.IJoin;
import org.gama.stalactite.query.model.From.AbstractJoin.JoinDirection;

/**
 * Class to ease From clause creation in a Select SQL statement.
 * - allow join declaration from Column
 * - fluent API
 * 
 * @author Guillaume Mary
 */
public class From implements Iterable<IJoin>, JoinChain {
	
	private final List<IJoin> joins = new ArrayList<>();
	
	private final Map<Table, String> tableAliases = new HashMap<>(4);
	
	public From() {
	}
	
	public List<IJoin> getJoins() {
		return joins;
	}
	
	public Map<Table, String> getTableAliases() {
		return tableAliases;
	}
	
	public From innerJoin(Column leftColumn, Column rightColumn) {
		return addNewJoin(leftColumn, null, rightColumn, null, JoinDirection.INNER_JOIN);
	}
	
	public From innerJoin(Column leftColumn, String leftTableAlias, Column rightColumn, String rightTableAlias) {
		return addNewJoin(leftColumn, leftTableAlias, rightColumn, rightTableAlias, JoinDirection.INNER_JOIN);
	}
	
	public From leftOuterJoin(Column leftColumn, Column rightColumn) {
		return addNewJoin(leftColumn, null, rightColumn, null, JoinDirection.LEFT_OUTER_JOIN);
	}
	
	public From leftOuterJoin(Column leftColumn, String leftTableAlias, Column rightColumn, String rightTableAlias) {
		return addNewJoin(leftColumn, leftTableAlias, rightColumn, rightTableAlias, JoinDirection.LEFT_OUTER_JOIN);
	}
	
	public From rightOuterJoin(Column leftColumn, Column rightColumn) {
		return addNewJoin(leftColumn, null, rightColumn, null, JoinDirection.RIGHT_OUTER_JOIN);
	}
	
	public From rightOuterJoin(Column leftColumn, String leftTableAlias, Column rightColumn, String rightTableAlias) {
		return addNewJoin(leftColumn, leftTableAlias, rightColumn, rightTableAlias, JoinDirection.RIGHT_OUTER_JOIN);
	}
	
	private From addNewJoin(Column leftColumn, String leftTableAlias, Column rightColumn, String rightTableAlias, JoinDirection joinDirection) {
		return add(new ColumnJoin(leftColumn, leftTableAlias, rightColumn, rightTableAlias, joinDirection));
	}
	
	public From innerJoin(Table leftTable, Table rightTable, String joinClause) {
		return addNewJoin(leftTable, null, rightTable, null, joinClause, JoinDirection.INNER_JOIN);
	}
	
	public From innerJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause) {
		return addNewJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, JoinDirection.INNER_JOIN);
	}
	
	public From leftOuterJoin(Table leftTable, Table rigTable, String joinClause) {
		return addNewJoin(leftTable, null, rigTable, null, joinClause, JoinDirection.LEFT_OUTER_JOIN);
	}
	
	public From leftOuterJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause) {
		return addNewJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, JoinDirection.LEFT_OUTER_JOIN);
	}
	
	public From rightOuterJoin(Table leftTable, Table rigTable, String joinClause) {
		return addNewJoin(leftTable, null, rigTable, null, joinClause, JoinDirection.RIGHT_OUTER_JOIN);
	}
	
	public From rightOuterJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause) {
		return addNewJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, JoinDirection.RIGHT_OUTER_JOIN);
	}
	
	public From crossJoin(Table table) {
		CrossJoin crossJoin = new CrossJoin(table);
		add(crossJoin);
		return this;
	}
	
	public From crossJoin(Table table, String tableAlias) {
		CrossJoin crossJoin = new CrossJoin(table, tableAlias);
		add(crossJoin);
		return this;
	}
	
	public From add(Table table) {
		CrossJoin crossJoin = new CrossJoin(table);
		return add(crossJoin);
	}
	
	public From add(Table table, String tableAlias) {
		CrossJoin crossJoin = new CrossJoin(table, tableAlias);
		return add(crossJoin);
	}
	
	private From addNewJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause, JoinDirection joinDirection) {
		return add(new RawTableJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, joinDirection));
	}
	
	/**
	 * More manual way to add join to this
	 *
	 * @param join the join to be added
	 * @return this
	 */
	public From add(IJoin join) {
		this.joins.add(join);
		return this;
	}
	
	private void addAlias(Table table, String alias) {
		if (!Strings.isEmpty(alias)) {
			this.tableAliases.put(table, alias);
		}
	}
	
	@Override
	public Iterator<IJoin> iterator() {
		return this.joins.iterator();
	}
	
	/**
	 * A representation of a table with an optional alias.
	 * It's auto-added to the table aliases Map of the {@link From} embbeding instance
	 * 
	 * @see #addAlias(Table, String) 
	 */
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
	}
	
	/**
	 * Small contract for join
	 */
	public interface IJoin {
		
		AliasedTable getLeftTable();
	}
	
	/**
	 * Parent class for join
	 */
	public abstract static class AbstractJoin implements IJoin {
		
		public enum JoinDirection {
			INNER_JOIN,
			LEFT_OUTER_JOIN,
			RIGHT_OUTER_JOIN
		}
		
		private final JoinDirection joinDirection;
		
		/**
		 * Contructor that needs direction
		 *
		 * @param joinDirection the join type of this join
		 */
		protected AbstractJoin(JoinDirection joinDirection) {
			this.joinDirection = joinDirection;
		}
		
		public JoinDirection getJoinDirection() {
			return this.joinDirection;
		}
		
		public boolean isInner() {
			return joinDirection == JoinDirection.INNER_JOIN;
		}
		
		public boolean isLeftOuter() {
				return joinDirection == JoinDirection.LEFT_OUTER_JOIN;
		}
		
		public boolean isRightOuter() {
			return joinDirection == JoinDirection.RIGHT_OUTER_JOIN;
		}
		
		public abstract AliasedTable getRightTable();
	}
	
	/**
	 * A class for cross join with some fluent API to chain with other kind of join
	 */
	public class CrossJoin implements IJoin {
		
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
		
		public From crossJoin(Table table) {
			CrossJoin crossJoin = new CrossJoin(table);
			return From.this.add(crossJoin);
		}
		
		public From crossJoin(Table table, String alias) {
			CrossJoin crossJoin = new CrossJoin(table, alias);
			return From.this.add(crossJoin);
		}
	}
	
	/**
	 * Join that only ask for joined table but not the way they are : the join condition is a free and is let as a String
	 */
	public class RawTableJoin extends AbstractJoin {
		private final AliasedTable leftTable;
		private final AliasedTable rightTable;
		private final String joinClause;
		
		public RawTableJoin(Table leftTable, Table rightTable, String joinClause, JoinDirection joinDirection) {
			super(joinDirection);
			this.leftTable = new AliasedTable(leftTable);
			this.joinClause = joinClause;
			this.rightTable = new AliasedTable(rightTable);
		}
		
		public RawTableJoin(Table leftTable, String leftTableAlias,
							Table rightTable, String rightTableAlias, String joinClause, JoinDirection joinDirection) {
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
	
	/**
	 * Class that define a join with {@link Column}
	 */
	public class ColumnJoin extends AbstractJoin {
		
		private final Column leftColumn;
		private final Column rightColumn;
		private final AliasedTable leftTable;
		private final AliasedTable rightTable;
		
		public ColumnJoin(Column leftColumn, Column rightColumn, JoinDirection joinDirection) {
			this(leftColumn, null, rightColumn, null, joinDirection);
		}
		
		public ColumnJoin(Column leftColumn, String leftAlias, Column rightColumn, String rightAlias, JoinDirection joinDirection) {
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
