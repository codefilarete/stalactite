package org.gama.stalactite.persistence.engine.runtime.load;

import org.gama.lang.collection.ReadOnlyList;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface JoinNode<T extends Table> {
	
	T getTable();
	
	/**
	 * Return elements joined to this node. Those can only be of type {@link AbstractJoinNode} since it can't be {@link JoinRoot}.
	 * 
	 * @return joins associated to this node
	 */
	ReadOnlyList<AbstractJoinNode> getJoins();
	
	void add(AbstractJoinNode node);
}
