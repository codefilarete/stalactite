package org.codefilarete.stalactite.query.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codefilarete.stalactite.query.model.From.AbstractJoin.JoinDirection;
import org.codefilarete.stalactite.query.model.From.Join;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.collection.IdentityMap;

import static org.codefilarete.stalactite.query.model.From.AbstractJoin.JoinDirection.*;

/**
 * Class to ease From clause creation in a Select SQL statement.
 * - allow join declaration from Column
 * - fluent API
 * 
 * @author Guillaume Mary
 */
public class From implements Iterable<Join>, JoinChain<From> {
	
	private Fromable root;
	
	private final List<Join> joins = new ArrayList<>();
	
	/**
	 * Table aliases.
	 * An {@link IdentityMap} is used to support presence of table clone : same name but not same instance. Needed in particular for cycling.
	 */
	private final IdentityMap<Fromable, String> tableAliases = new IdentityMap<>(4);
	
	/**
	 * @param root element to be used as root, can be null but be sure to use {@link #setRoot(Fromable)} later on
	 */
	public From(Fromable root) {
		this.root = root;
	}
	
	/**
	 * @param root element to be used as root, can be null but be sure to use {@link #setRoot(Fromable, String)} later on
	 * @param rootAlias alias of root element
	 */
	public From(Fromable root, String rootAlias) {
		setRoot(root, rootAlias);
	}
	
	public Fromable getRoot() {
		return root;
	}
	
	/**
	 * Sets very first "table" of the clause. Expected to be used only if no-arg constructor was used (else you may break your query)
	 * @param root element to be used as root
	 * @return this
	 */
	public From setRoot(Fromable root) {
		this.root = root;
		return this;
	}
	
	/**
	 * Sets very first "table" of the clause. Expected to be used only if no-arg constructor was used (else you may break your query)
	 * @param root element to be used as root
	 * @param rootAlias alias of root element
	 * @return this
	 */
	public From setRoot(Fromable root, String rootAlias) {
		this.root = root;
		this.setAlias(root, rootAlias);
		return this;
	}
	
	public List<Join> getJoins() {
		return joins;
	}
	
	public IdentityMap<Fromable, String> getTableAliases() {
		return tableAliases;
	}
	
	@Override
	public <I> From innerJoin(JoinLink<?, I> leftColumn, JoinLink<?, I> rightColumn) {
		return addNewJoin(leftColumn, rightColumn, INNER_JOIN);
	}
	
	@Override
	public <JOINTYPE> From innerJoin(Key<?, JOINTYPE> leftColumns, Key<?, JOINTYPE> rightColumns) {
		return addNewJoin(leftColumns, rightColumns, INNER_JOIN);
	}
	
	@Override
	public <I> From leftOuterJoin(JoinLink<?, I> leftColumn, JoinLink<?, I> rightColumn) {
		return addNewJoin(leftColumn, rightColumn, LEFT_OUTER_JOIN);
	}
	
	@Override
	public <JOINTYPE> From leftOuterJoin(Key<?, JOINTYPE> leftColumns, Key<?, JOINTYPE> rightColumns) {
		return addNewJoin(leftColumns, rightColumns, LEFT_OUTER_JOIN);
	}
	
	@Override
	public <I> From rightOuterJoin(JoinLink<?, I> leftColumn, JoinLink<?, I> rightColumn) {
		return addNewJoin(leftColumn, rightColumn, RIGHT_OUTER_JOIN);
	}
	
	private <I> From addNewJoin(JoinLink<?, I> leftColumn, JoinLink<?, I> rightColumn, JoinDirection joinDirection) {
		return add(new ColumnJoin<>(leftColumn, rightColumn, joinDirection));
	}
	
	private <JOINTYPE> From addNewJoin(Key<?, JOINTYPE> leftColumns, Key<?, JOINTYPE> rightColumns, JoinDirection joinDirection) {
		return add(new KeyJoin(leftColumns, rightColumns, joinDirection));
	}
	
	@Override
	public From innerJoin(Fromable rightTable, String joinClause) {
		return addNewJoin(rightTable, joinClause, INNER_JOIN);
	}
	
	@Override
	public From innerJoin(Fromable rightTable, String rightTableAlias, String joinClause) {
		return addNewJoin(rightTable, rightTableAlias, joinClause, INNER_JOIN);
	}
	
	@Override
	public From innerJoin(QueryProvider<?> rightTable, String rightTableAlias, String joinClause) {
		return innerJoin(rightTable.getQuery().asPseudoTable(), rightTableAlias, joinClause);
	}
	
	@Override
	public From leftOuterJoin(Fromable rightTable, String joinClause) {
		return addNewJoin(rightTable, joinClause, LEFT_OUTER_JOIN);
	}
	
	@Override
	public From leftOuterJoin(Fromable rightTable, String rightTableAlias, String joinClause) {
		return addNewJoin(rightTable, rightTableAlias, joinClause, LEFT_OUTER_JOIN);
	}
	
	@Override
	public From leftOuterJoin(QueryProvider<?> rightTable, String rightTableAlias, String joinClause) {
		return leftOuterJoin(rightTable.getQuery().asPseudoTable(), rightTableAlias, joinClause);
	}
	
	@Override
	public From rightOuterJoin(Fromable rightTable, String joinClause) {
		return addNewJoin(rightTable, joinClause, RIGHT_OUTER_JOIN);
	}
	
	@Override
	public From rightOuterJoin(Fromable rightTable, String rightTableAlias, String joinClause) {
		return addNewJoin(rightTable, rightTableAlias, joinClause, RIGHT_OUTER_JOIN);
	}
	
	@Override
	public From rightOuterJoin(QueryProvider<?> rightTable, String rightTableAlias, String joinClause) {
		return rightOuterJoin(rightTable.getQuery().asPseudoTable(), rightTableAlias, joinClause);
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
	
	@Override
	public From crossJoin(QueryProvider<?> query, String tableAlias) {
		CrossJoin crossJoin = new CrossJoin(query.getQuery().asPseudoTable(), tableAlias);
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
	
	private From addNewJoin(Fromable rightTable, String joinClause, JoinDirection joinDirection) {
		return add(new RawTableJoin(rightTable, null, joinClause, joinDirection));
	}
	
	private From addNewJoin(Fromable rightTable, String rightTableAlias, String joinClause, JoinDirection joinDirection) {
		return add(new RawTableJoin(rightTable, rightTableAlias, joinClause, joinDirection));
	}
	
	@Override
	public From setAlias(Fromable table, String alias) {
		if (!Strings.isEmpty(alias)) {
			this.tableAliases.put(table, alias);
		}
		return this;
	}
	
	/**
	 * Manual an internal way to add join to current instance
	 *
	 * @param join the join to be added
	 * @return this
	 */
	private From add(Join join) {
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
		
		Fromable getRightTable();
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
			return joinDirection == INNER_JOIN;
		}
		
		public boolean isLeftOuter() {
				return joinDirection == LEFT_OUTER_JOIN;
		}
		
		public boolean isRightOuter() {
			return joinDirection == RIGHT_OUTER_JOIN;
		}
	}
	
	/**
	 * A class for cross join with some fluent API to chain with other kind of join
	 */
	public class CrossJoin implements Join {
		
		private final Fromable rightTable;
		
		private CrossJoin(Fromable rightTable) {
			this.rightTable = rightTable;
		}
		
		private CrossJoin(Fromable table, String tableAlias) {
			this(table);
			setAlias(table, tableAlias);
		}
		
		@Override
		public Fromable getRightTable() {
			return this.rightTable;
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
		
		private final Fromable rightTable;
		private final String joinClause;
		
		private RawTableJoin(Fromable rightTable, String rightTableAlias, String joinClause, JoinDirection joinDirection) {
			super(joinDirection);
			this.rightTable = rightTable;
			this.joinClause = joinClause;
			setAlias(rightTable, rightTableAlias);
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
	public class ColumnJoin<I> extends AbstractJoin {
		
		private final JoinLink<?, I> leftColumn;
		private final JoinLink<?, I> rightColumn;
		
		private ColumnJoin(JoinLink<?, I> leftColumn, JoinLink<?, I> rightColumn, JoinDirection joinDirection) {
			super(joinDirection);
			this.leftColumn = leftColumn;
			this.rightColumn = rightColumn;
		}
		
		public JoinLink<?, I> getLeftColumn() {
			return leftColumn;
		}
		
		public JoinLink<?, I> getRightColumn() {
			return rightColumn;
		}
		
		@Override
		public Fromable getRightTable() {
			return rightColumn.getOwner();
		}
	}
	
	/**
	 * Class that defines a join with {@link Column}
	 */
	public class KeyJoin<JOINTYPE> extends AbstractJoin {
		
		private final Key<?, JOINTYPE> leftKey;
		private final Key<?, JOINTYPE> rightKey;
		
		private KeyJoin(Key<?, JOINTYPE> leftColumns, Key<?, JOINTYPE> rightColumns, JoinDirection joinDirection) {
			super(joinDirection);
			this.leftKey = leftColumns;
			this.rightKey = rightColumns;
		}
		
		public Key<?, JOINTYPE> getLeftKey() {
			return leftKey;
		}
		
		public Key<?, JOINTYPE> getRightKey() {
			return rightKey;
		}
		
		@Override
		public Fromable getRightTable() {
			return rightKey.getTable();
		}
	}
}
