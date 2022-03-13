package org.codefilarete.stalactite.persistence.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.Set;

import org.codefilarete.tool.collection.ReadOnlyList;
import org.codefilarete.stalactite.persistence.mapping.ColumnedRow;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface JoinNode<T extends Table> {
	
	T getTable();
	
	Set<Column<T, Object>> getColumnsToSelect();
	
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
	
	JoinRowConsumer toConsumer(ColumnedRow columnedRow);
}
