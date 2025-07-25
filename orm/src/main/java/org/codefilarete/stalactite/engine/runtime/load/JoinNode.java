package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.Set;

import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.tool.collection.ReadOnlyList;

/**
 * @author Guillaume Mary
 */
public interface JoinNode<C, T extends Fromable> {
	
	T getTable();
	
	Set<Selectable<?>> getColumnsToSelect();
	
	@Nullable
	String getTableAlias();
	
	/**
	 * Return elements joined to this node. Those can only be of type {@link AbstractJoinNode} since it can't be {@link JoinRoot}.
	 * 
	 * @return joins associated to this node
	 */
	ReadOnlyList<AbstractJoinNode> getJoins();
	
	void add(AbstractJoinNode node);
	
	/**
	 * Gives {@link EntityJoinTree} that owns this node
	 * May needs some computation if this node is a high depth one.
	 * 
	 * @param <ROOT> tree entity type 
	 * @param <ID> tree entity identifier type
	 * @return {@link EntityJoinTree} that owns this node, never null
	 */
	<ROOT, ID> EntityJoinTree<ROOT, ID> getTree();
	
	JoinRowConsumer toConsumer(JoinNode<C, T> joinNode);
	
	JoinNode<C, T> setConsumptionListener(@Nullable EntityTreeJoinNodeConsumptionListener<C> consumptionListener);
}
