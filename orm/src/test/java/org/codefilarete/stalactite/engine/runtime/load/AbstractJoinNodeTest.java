package org.codefilarete.stalactite.engine.runtime.load;

import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class AbstractJoinNodeTest {
	
	@Test
	void getTree() {
		EntityJoinTree<Object, Object> tree = new EntityJoinTree<>(null, null);
		assertThat(tree.getRoot().getTree()).isSameAs(tree);
		
		RelationJoinNode<Object, Table, Table, Object, Object> node1 = new RelationJoinNode<>(tree.getRoot(), (JoinLink) Mockito.mock(JoinLink.class), (JoinLink) Mockito.mock(JoinLink.class), null, null, null, null, null, null);
		assertThat(node1.getTree()).isSameAs(tree);
		
		RelationJoinNode<Object, Table, Table, Object, Object> node2 = new RelationJoinNode<>(node1, (JoinLink) Mockito.mock(JoinLink.class), (JoinLink) Mockito.mock(JoinLink.class), null, null, null, null, null, null);
		assertThat(node2.getTree()).isSameAs(tree);
		
	}
}