package org.gama.stalactite.query.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codefilarete.tool.Strings;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.IdentityMap;
import org.gama.stalactite.query.model.From.AbstractJoin.JoinDirection;
import org.gama.stalactite.query.model.From.Join;

/**
 * Class to ease From clause creation in a Select SQL statement.
 * - allow join declaration from Column
 * - fluent API
 * 
 * @author Guillaume Mary
 */
public class From implements Iterable<Join>, JoinChain {
	
	private final List<Join> joins = new ArrayList<>();
	
	/**
	 * Table aliases.
	 * An {@link IdentityMap} is used to support presence of table clone : same name but not same instance. Needed in particular for cycling.
	 */
	private final IdentityMap<Table, String> tableAliases = new IdentityMap<>(4);
	
	public From() {
		// default constructor, properties are already assigned
	}
	
	public List<Join> getJoins() {
		return joins;
	}
	
	public IdentityMap<Table, String> getTableAliases() {
		return tableAliases;
	}
	
	@Override
	public From innerJoin(Column leftColumn, Column rightColumn) {
		return addNewJoin(leftColumn, rightColumn, JoinDirection.INNER_JOIN);
	}
	
	@Override
	public From leftOuterJoin(Column leftColumn, Column rightColumn) {
		return addNewJoin(leftColumn, rightColumn, JoinDirection.LEFT_OUTER_JOIN);
	}
	
	@Override
	public From rightOuterJoin(Column leftColumn, Column rightColumn) {
		return addNewJoin(leftColumn, rightColumn, JoinDirection.RIGHT_OUTER_JOIN);
	}
	
	private From addNewJoin(Column leftColumn, Column rightColumn, JoinDirection joinDirection) {
		return add(new ColumnJoin(leftColumn, rightColumn, joinDirection));
	}
	
	@Override
	public From innerJoin(Table leftTable, Table rightTable, String joinClause) {
		return addNewJoin(leftTable, rightTable, joinClause, JoinDirection.INNER_JOIN);
	}
	
	@Override
	public From innerJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause) {
		return addNewJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, JoinDirection.INNER_JOIN);
	}
	
	@Override
	public From leftOuterJoin(Table leftTable, Table rigTable, String joinClause) {
		return addNewJoin(leftTable, rigTable, joinClause, JoinDirection.LEFT_OUTER_JOIN);
	}
	
	@Override
	public From leftOuterJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause) {
		return addNewJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, JoinDirection.LEFT_OUTER_JOIN);
	}
	
	@Override
	public From rightOuterJoin(Table leftTable, Table rigTable, String joinClause) {
		return addNewJoin(leftTable, rigTable, joinClause, JoinDirection.RIGHT_OUTER_JOIN);
	}
	
	@Override
	public From rightOuterJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause) {
		return addNewJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, JoinDirection.RIGHT_OUTER_JOIN);
	}
	
	@Override
	public From crossJoin(Table table) {
		CrossJoin crossJoin = new CrossJoin(table);
		add(crossJoin);
		return this;
	}
	
	@Override
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
	
	private From addNewJoin(Table leftTable, Table rigTable, String joinClause, JoinDirection joinDirection) {
		return add(new RawTableJoin(leftTable, rigTable, joinClause, joinDirection));
	}
	
	private From addNewJoin(Table leftTable, String leftTableAlias, Table rigTable, String rightTableAlias, String joinClause, JoinDirection joinDirection) {
		return add(new RawTableJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, joinDirection));
	}
	
	@Override
	public From setAlias(Table table, String alias) {
		if (!Strings.isEmpty(alias)) {
			this.tableAliases.put(table, alias);
		}
		return this;
	}
	
	/**
	 * More manual way to add join to this
	 *
	 * @param join the join to be added
	 * @return this
	 */
	public From add(Join join) {
		this.joins.add(join);
		return this;
	}
	
	@Override
	public Iterator<Join> iterator() {
		return this.joins.iterator();
	}
	
	/**
	 * Small contract of a join
	 */
	public interface Join {
		
		Table getLeftTable();
	}
	
	/**
	 * Parent class for join
	 */
	public abstract static class AbstractJoin implements Join {
		
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
		
		public abstract Table getRightTable();
	}
	
	/**
	 * A class for cross join with some fluent API to chain with other kind of join
	 */
	public class CrossJoin implements Join {
		
		private final Table leftTable;
		
		public CrossJoin(Table leftTable) {
			this.leftTable = leftTable;
		}
		
		public CrossJoin(Table table, String tableAlias) {
			this(table);
			setAlias(table, tableAlias);
		}
		
		@Override
		public Table getLeftTable() {
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
		private final Table leftTable;
		private final Table rightTable;
		private final String joinClause;
		
		public RawTableJoin(Table leftTable, Table rightTable, String joinClause, JoinDirection joinDirection) {
			this(leftTable, null, rightTable, null, joinClause, joinDirection);
		}
		
		public RawTableJoin(Table leftTable, String leftTableAlias,
							Table rightTable, String rightTableAlias, String joinClause, JoinDirection joinDirection) {
			super(joinDirection);
			this.leftTable = leftTable;
			this.rightTable = rightTable;
			this.joinClause = joinClause;
			setAlias(leftTable, leftTableAlias);
			setAlias(rightTable, rightTableAlias);
		}
		
		@Override
		public Table getLeftTable() {
			return leftTable;
		}
		
		@Override
		public Table getRightTable() {
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
		
		public ColumnJoin(Column leftColumn, Column rightColumn, JoinDirection joinDirection) {
			super(joinDirection);
			this.leftColumn = leftColumn;
			this.rightColumn = rightColumn;
		}
		
		public Column getLeftColumn() {
			return leftColumn;
		}
		
		public Column getRightColumn() {
			return rightColumn;
		}
		
		@Override
		public Table getLeftTable() {
			return leftColumn.getTable();
		}
		
		@Override
		public Table getRightTable() {
			return rightColumn.getTable();
		}
	}
}
