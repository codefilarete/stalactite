package org.codefilarete.stalactite.query.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codefilarete.stalactite.query.builder.IdentityMap;
import org.codefilarete.stalactite.query.model.From.AbstractJoin.JoinDirection;
import org.codefilarete.stalactite.query.model.From.Join;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.Strings;

/**
 * Class to ease From clause creation in a Select SQL statement.
 * - allow join declaration from Column
 * - fluent API
 * 
 * @author Guillaume Mary
 */
public class From implements Iterable<Join>, JoinChain<From> {
	
	private final List<Join> joins = new ArrayList<>();
	
	/**
	 * Table aliases.
	 * An {@link IdentityMap} is used to support presence of table clone : same name but not same instance. Needed in particular for cycling.
	 */
	private final IdentityMap<Fromable, String> tableAliases = new IdentityMap<>(4);
	
	public From() {
		// default constructor, properties are already assigned
	}
	
	public List<Join> getJoins() {
		return joins;
	}
	
	public IdentityMap<Fromable, String> getTableAliases() {
		return tableAliases;
	}
	
	@Override
	public <I> From innerJoin(JoinLink<I> leftColumn, JoinLink<I> rightColumn) {
		return addNewJoin(leftColumn, rightColumn, JoinDirection.INNER_JOIN);
	}
	
	@Override
	public <I> From leftOuterJoin(JoinLink<I> leftColumn, JoinLink<I> rightColumn) {
		return addNewJoin(leftColumn, rightColumn, JoinDirection.LEFT_OUTER_JOIN);
	}
	
	@Override
	public <I> From rightOuterJoin(JoinLink<I> leftColumn, JoinLink<I> rightColumn) {
		return addNewJoin(leftColumn, rightColumn, JoinDirection.RIGHT_OUTER_JOIN);
	}
	
	private <I> From addNewJoin(JoinLink<I> leftColumn, JoinLink<I> rightColumn, JoinDirection joinDirection) {
		return add(new ColumnJoin(leftColumn, rightColumn, joinDirection));
	}
	
	@Override
	public From innerJoin(Fromable leftTable, Fromable rightTable, String joinClause) {
		return addNewJoin(leftTable, rightTable, joinClause, JoinDirection.INNER_JOIN);
	}
	
	@Override
	public From innerJoin(Fromable leftTable, String leftTableAlias, Fromable rigTable, String rightTableAlias, String joinClause) {
		return addNewJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, JoinDirection.INNER_JOIN);
	}
	
	@Override
	public From leftOuterJoin(Fromable leftTable, Fromable rigTable, String joinClause) {
		return addNewJoin(leftTable, rigTable, joinClause, JoinDirection.LEFT_OUTER_JOIN);
	}
	
	@Override
	public From leftOuterJoin(Fromable leftTable, String leftTableAlias, Fromable rigTable, String rightTableAlias, String joinClause) {
		return addNewJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, JoinDirection.LEFT_OUTER_JOIN);
	}
	
	@Override
	public From rightOuterJoin(Fromable leftTable, Fromable rigTable, String joinClause) {
		return addNewJoin(leftTable, rigTable, joinClause, JoinDirection.RIGHT_OUTER_JOIN);
	}
	
	@Override
	public From rightOuterJoin(Fromable leftTable, String leftTableAlias, Fromable rigTable, String rightTableAlias, String joinClause) {
		return addNewJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, JoinDirection.RIGHT_OUTER_JOIN);
	}
	
	@Override
	public From crossJoin(Fromable table) {
		CrossJoin crossJoin = new CrossJoin(table);
		add(crossJoin);
		return this;
	}
	
	@Override
	public From crossJoin(Fromable table, String tableAlias) {
		CrossJoin crossJoin = new CrossJoin(table, tableAlias);
		add(crossJoin);
		return this;
	}
	
	public From add(Fromable table) {
		CrossJoin crossJoin = new CrossJoin(table);
		return add(crossJoin);
	}
	
	public From add(Fromable table, String tableAlias) {
		CrossJoin crossJoin = new CrossJoin(table, tableAlias);
		return add(crossJoin);
	}
	
	private From addNewJoin(Fromable leftTable, Fromable rigTable, String joinClause, JoinDirection joinDirection) {
		return add(new RawTableJoin(leftTable, rigTable, joinClause, joinDirection));
	}
	
	private From addNewJoin(Fromable leftTable, String leftTableAlias, Fromable rigTable, String rightTableAlias, String joinClause, JoinDirection joinDirection) {
		return add(new RawTableJoin(leftTable, leftTableAlias, rigTable, rightTableAlias, joinClause, joinDirection));
	}
	
	@Override
	public From setAlias(Fromable table, String alias) {
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
		
		Fromable getLeftTable();
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
		 * Constructor that needs direction
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
		
		public abstract Fromable getRightTable();
	}
	
	/**
	 * A class for cross join with some fluent API to chain with other kind of join
	 */
	public class CrossJoin implements Join {
		
		private final Fromable leftTable;
		
		public CrossJoin(Fromable leftTable) {
			this.leftTable = leftTable;
		}
		
		public CrossJoin(Fromable table, String tableAlias) {
			this(table);
			setAlias(table, tableAlias);
		}
		
		@Override
		public Fromable getLeftTable() {
			return this.leftTable;
		}
		
		public From crossJoin(Fromable table) {
			CrossJoin crossJoin = new CrossJoin(table);
			return From.this.add(crossJoin);
		}
		
		public From crossJoin(Fromable table, String alias) {
			CrossJoin crossJoin = new CrossJoin(table, alias);
			return From.this.add(crossJoin);
		}
	}
	
	/**
	 * Join that only ask for joined table but not the way they are : the join condition is a free and is let as a String
	 */
	public class RawTableJoin extends AbstractJoin {
		private final Fromable leftTable;
		private final Fromable rightTable;
		private final String joinClause;
		
		public RawTableJoin(Fromable leftTable, Fromable rightTable, String joinClause, JoinDirection joinDirection) {
			this(leftTable, null, rightTable, null, joinClause, joinDirection);
		}
		
		public RawTableJoin(Fromable leftTable, String leftTableAlias,
							Fromable rightTable, String rightTableAlias, String joinClause, JoinDirection joinDirection) {
			super(joinDirection);
			this.leftTable = leftTable;
			this.rightTable = rightTable;
			this.joinClause = joinClause;
			setAlias(leftTable, leftTableAlias);
			setAlias(rightTable, rightTableAlias);
		}
		
		@Override
		public Fromable getLeftTable() {
			return leftTable;
		}
		
		@Override
		public Fromable getRightTable() {
			return rightTable;
		}
		
		public String getJoinClause() {
			return joinClause;
		}
	}
	
	/**
	 * Class that defines a join with {@link Column}
	 */
	public class ColumnJoin extends AbstractJoin {
		
		private final JoinLink leftColumn;
		private final JoinLink rightColumn;
		
		public ColumnJoin(JoinLink leftColumn, JoinLink rightColumn, JoinDirection joinDirection) {
			super(joinDirection);
			this.leftColumn = leftColumn;
			this.rightColumn = rightColumn;
		}
		
		public JoinLink getLeftColumn() {
			return leftColumn;
		}
		
		public JoinLink getRightColumn() {
			return rightColumn;
		}
		
		@Override
		public Fromable getLeftTable() {
			return leftColumn.getOwner();
		}
		
		@Override
		public Fromable getRightTable() {
			return rightColumn.getOwner();
		}
	}
}
