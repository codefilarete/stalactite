package org.gama.stalactite.persistence.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.gama.lang.collection.ReadOnlyList;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Abstraction of relation, merge and passive joins.
 * 
 * @author Guillaume Mary
 */
public abstract class AbstractJoinNode<C, T1 extends Table, T2 extends Table, I> implements JoinNode<T2> {
	
	/** Join column with previous strategy table */
	private final Column<T1, I> leftJoinColumn;
	
	/** Join column with next strategy table */
	private final Column<T2, I> rightJoinColumn;
	
	/** Indicates if the join must be an inner or (left) outer join */
	private final JoinType joinType;
	
	private final Set<Column<T2, Object>> columnsToSelect;
	
	private final JoinNode<T1> parent;
	
	/** Joins */
	private final List<AbstractJoinNode> joins = new ArrayList<>();
	
	@Nullable
	protected String tableAlias;
	
	protected AbstractJoinNode(JoinNode<T1> parent,
							   Column<T1, I> leftJoinColumn,
							   Column<T2, I> rightJoinColumn,
							   JoinType joinType,
							   Set<Column<T2, Object>> columnsToSelect,
							   @Nullable String tableAlias) {
		this.parent = parent;
		this.leftJoinColumn = leftJoinColumn;
		this.rightJoinColumn = rightJoinColumn;
		this.joinType = joinType;
		this.columnsToSelect = columnsToSelect;
		this.tableAlias = tableAlias;
		parent.add(this);
	}
	
	public JoinNode<T1> getParent() {
		return parent;
	}
	
	@Override
	public T2 getTable() {
		return getRightTable();
	}
	
	public Column<?, I> getLeftJoinColumn() {
		return leftJoinColumn;
	}
	
	public Column<T2, I> getRightJoinColumn() {
		return rightJoinColumn;
	}
	
	public JoinType getJoinType() {
		return joinType;
	}
	
	@Override
	public Set<Column<T2, Object>> getColumnsToSelect() {
		return columnsToSelect;
	}
	
	@Override
	public ReadOnlyList<AbstractJoinNode> getJoins() {
		return new ReadOnlyList<>(joins);
	}
	
	@Override
	public void add(AbstractJoinNode node) {
		// safeguard
		if (node.getParent() != this) {
			throw new IllegalArgumentException("Node is not added as child of right node : parent differs from target owner");
		}
		this.joins.add(node);
	}
	
	@Nullable
	@Override
	public String getTableAlias() {
		return tableAlias;
	}
	
	public T2 getRightTable() {
		return this.rightJoinColumn.getTable();
	}
	
}
