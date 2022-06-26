package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.tool.collection.ReadOnlyList;

/**
 * Abstraction of relation, merge and passive joins.
 * 
 * @author Guillaume Mary
 */
public abstract class AbstractJoinNode<C, T1 extends Fromable, T2 extends Fromable, JOINCOLTYPE> implements JoinNode<T2> {
	
	/** Join column with previous strategy table */
	private final JoinLink<T1, JOINCOLTYPE> leftJoinColumn;
	
	/** Join column with next strategy table */
	private final JoinLink<T2, JOINCOLTYPE> rightJoinColumn;
	
	/** Indicates if the join must be an inner or (left) outer join */
	private final JoinType joinType;
	
	private final Set<Selectable<Object>> columnsToSelect;
	
	private final JoinNode<T1> parent;
	
	/** Joins */
	private final List<AbstractJoinNode> joins = new ArrayList<>();
	
	@Nullable
	protected String tableAlias;
	
	@Nullable
	private TransformerListener<C> transformerListener;
	
	protected AbstractJoinNode(JoinNode<T1> parent,
							   JoinLink<T1, JOINCOLTYPE> leftJoinColumn,
							   JoinLink<T2, JOINCOLTYPE> rightJoinColumn,
							   JoinType joinType,
							   Set<? extends Selectable<?>> columnsToSelect,	// From T2
							   @Nullable String tableAlias) {
		this.parent = parent;
		this.leftJoinColumn = leftJoinColumn;
		this.rightJoinColumn = rightJoinColumn;
		this.joinType = joinType;
		this.columnsToSelect = (Set) columnsToSelect;
		this.tableAlias = tableAlias;
		parent.add(this);
	}
	
	@Override
	public <ROOT, ID> EntityJoinTree<ROOT, ID> getTree() {
		// going up to the root to get tree from it because JoinRoot owns the information
		JoinNodeHierarchyIterator joinNodeHierarchyIterator = new JoinNodeHierarchyIterator(this);
		AbstractJoinNode currentNode = this;
		while (joinNodeHierarchyIterator.hasNext()) {
			currentNode = joinNodeHierarchyIterator.next();
		}
		// currentNode is the last before root
		return currentNode.getParent().getTree();
	} 
	
	public JoinNode<T1> getParent() {
		return parent;
	}
	
	@Override
	public Fromable getTable() {
		return getRightTable();
	}
	
	public JoinLink<T1, JOINCOLTYPE> getLeftJoinColumn() {
		return leftJoinColumn;
	}
	
	public JoinLink<T2, JOINCOLTYPE> getRightJoinColumn() {
		return rightJoinColumn;
	}
	
	public JoinType getJoinType() {
		return joinType;
	}
	
	@Override
	public Set<Selectable<?>> getColumnsToSelect() {
		return (Set) columnsToSelect;
	}
	
	@Override
	public ReadOnlyList<AbstractJoinNode> getJoins() {
		return new ReadOnlyList<>(joins);
	}
	
	@Nullable
	TransformerListener<C> getTransformerListener() {
		return transformerListener;
	}
	
	public AbstractJoinNode<C, T1, T2, JOINCOLTYPE> setTransformerListener(@Nullable TransformerListener<C> transformerListener) {
		this.transformerListener = transformerListener;
		return this;
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
		return this.rightJoinColumn.getOwner();
	}
	
	/**
	 * Iterator over {@link AbstractJoinNode} from given node over parents, except root (because it's not a {@link AbstractJoinNode} so can't be returned)
	 */
	static class JoinNodeHierarchyIterator implements Iterator<AbstractJoinNode> {
		
		private AbstractJoinNode currentNode;
		
		JoinNodeHierarchyIterator(AbstractJoinNode currentNode) {
			this.currentNode = currentNode;
		}
		
		@Override
		public boolean hasNext() {
			return currentNode != null;
		}
		
		@Override
		public AbstractJoinNode next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			AbstractJoinNode toReturn = currentNode;
			prepareNextIteration();
			return toReturn;
		}
		
		private void prepareNextIteration() {
			JoinNode parent = currentNode.getParent();
			if (parent instanceof AbstractJoinNode) {
				currentNode = (AbstractJoinNode) parent;
			} else {
				currentNode = null;
			}
		}
	}
}
