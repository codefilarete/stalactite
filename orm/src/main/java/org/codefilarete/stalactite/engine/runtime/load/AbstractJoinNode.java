package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.tool.collection.ReadOnlyList;

/**
 * Abstraction of relation, merge and passive joins.
 * 
 * @author Guillaume Mary
 */
public abstract class AbstractJoinNode<C, T1 extends Fromable, T2 extends Fromable, JOINTYPE> implements JoinNode<C, T2> {
	
	/** Join column with previous strategy table */
	private final Key<T1, JOINTYPE> leftJoinLink;
	
	/** Join column with next strategy table */
	private final Key<T2, JOINTYPE> rightJoinLink;
	
	/** Indicates if the join must be an inner or (left) outer join */
	private final JoinType joinType;
	
	private final Set<Selectable<?>> columnsToSelect;
	
	private final JoinNode<?, T1> parent;
	
	/** Joins */
	private final List<AbstractJoinNode<?, ?, ?, ?>> joins = new ArrayList<>();
	
	@Nullable
	protected String tableAlias;
	
	@Nullable
	private EntityTreeJoinNodeConsumptionListener<C> consumptionListener;

	private final IdentityHashMap<JoinLink<?, ?>, JoinLink<?, ?>> originalColumnsToLocalOnes;

	protected AbstractJoinNode(JoinNode<?, T1> parent,
							   JoinLink<T1, JOINTYPE> leftJoinLink,
							   JoinLink<T2, JOINTYPE> rightJoinLink,
							   JoinType joinType,
							   Set<? extends Selectable<?>> columnsToSelect,	// From T2
							   @Nullable String tableAlias) {
		this(parent, Key.ofSingleColumn(leftJoinLink), Key.ofSingleColumn(rightJoinLink), joinType, columnsToSelect, tableAlias);
	}
	
	protected AbstractJoinNode(JoinNode<?, T1> parent,
							   Key<T1, JOINTYPE> leftJoinLink,
							   Key<T2, JOINTYPE> rightJoinLink,
							   JoinType joinType,
							   Set<? extends Selectable<?>> columnsToSelect,	// From T2
							   @Nullable String tableAlias) {
		this.parent = parent;
		this.leftJoinLink = leftJoinLink;
		this.rightJoinLink = rightJoinLink;
		this.joinType = joinType;
		this.columnsToSelect = (Set) columnsToSelect;
		this.tableAlias = tableAlias;
		parent.add(this);
		this.originalColumnsToLocalOnes = new IdentityHashMap<>();
		rightJoinLink.getTable().getColumns().forEach(column -> {
			// we clone columns to avoid side effects on the original query
			this.originalColumnsToLocalOnes.put((JoinLink<?, ?>) column, (JoinLink<?, ?>) column);
		});
	}

	/**
	 * Constructor dedicated for node cloning
	 * 
	 * @param parent the node to add the created instance to
	 * @param leftJoinLink the left joining columns of this node, expected to be taken on parent node table
	 * @param rightJoinLink the right joining columns of this node, expected to be taken on this node table
	 * @param joinType inner or outer join type
	 * @param columnsToSelect additional columns to select from right table (T2), out of the joining columns which are automatically selected
	 * @param tableAlias optional table alias to use in the query, if null then no alias is used
	 * @param originalColumnsToLocalOnes a map of original columns to local ones, used to simplify and allow selecting original columns by user
	 */
	protected AbstractJoinNode(JoinNode<?, T1> parent,
							   Key<T1, JOINTYPE> leftJoinLink,
							   Key<T2, JOINTYPE> rightJoinLink,
							   JoinType joinType,
							   Set<? extends Selectable<?>> columnsToSelect,	// From T2
							   @Nullable String tableAlias,
							   IdentityHashMap<JoinLink<?, ?>, JoinLink<?, ?>> originalColumnsToLocalOnes) {
		this.parent = parent;
		this.leftJoinLink = leftJoinLink;
		this.rightJoinLink = rightJoinLink;
		this.joinType = joinType;
		this.columnsToSelect = (Set) columnsToSelect;
		this.tableAlias = tableAlias;
		parent.add(this);
		this.originalColumnsToLocalOnes = originalColumnsToLocalOnes;
	}
	
	@Override
	public <ROOT, ID> EntityJoinTree<ROOT, ID> getTree() {
		// going up to the root to get tree from it because JoinRoot owns the information
		JoinNodeHierarchyIterator joinNodeHierarchyIterator = new JoinNodeHierarchyIterator(this);
		AbstractJoinNode<?, ?, ?, ?> currentNode = this;
		while (joinNodeHierarchyIterator.hasNext()) {
			currentNode = joinNodeHierarchyIterator.next();
		}
		// currentNode is the last before root
		return currentNode.getParent().getTree();
	} 
	
	public JoinNode<?, T1> getParent() {
		return parent;
	}
	
	@Override
	public T2 getTable() {
		return getRightTable();
	}
	
	public Key<T1, JOINTYPE> getLeftJoinLink() {
		return leftJoinLink;
	}
	
	public Key<T2, JOINTYPE> getRightJoinLink() {
		return rightJoinLink;
	}
	
	public JoinType getJoinType() {
		return joinType;
	}
	
	@Override
	public Set<Selectable<?>> getColumnsToSelect() {
		return columnsToSelect;
	}
	
	@Override
	public IdentityHashMap<JoinLink<?, ?>, JoinLink<?, ?>> getOriginalColumnsToLocalOnes() {
		return originalColumnsToLocalOnes;
	}

	@Override
	public ReadOnlyList<AbstractJoinNode<?, ?, ?, ?>> getJoins() {
		return new ReadOnlyList<>(joins);
	}
	
	@Nullable
	EntityTreeJoinNodeConsumptionListener<C> getConsumptionListener() {
		return consumptionListener;
	}
	
	@Override
	public AbstractJoinNode<C, T1, T2, JOINTYPE> setConsumptionListener(@Nullable EntityTreeJoinNodeConsumptionListener<C> consumptionListener) {
		this.consumptionListener = consumptionListener;
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
		return this.rightJoinLink.getTable();
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
