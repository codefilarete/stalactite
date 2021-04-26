package org.gama.stalactite.persistence.engine.runtime.load;

import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class AbstractJoinNodeTest {
	
	@Test
	void getTree() {
		EntityJoinTree<Object, Object> tree = new EntityJoinTree<>(null, null);
		assertThat(tree.getRoot().getTree()).isSameAs(tree);
		
		RelationJoinNode<Object, Table, Table, Object> node1 = new RelationJoinNode<>((JoinNode<Table>) tree.getRoot(), null, null, null, null, null, null, null, null);
		assertThat(node1.getTree()).isSameAs(tree);
		
		RelationJoinNode<Object, Table, Table, Object> node2 = new RelationJoinNode<>(node1, null, null, null, null, null, null, null, null);
		assertThat(node2.getTree()).isSameAs(tree);
		
	}
}