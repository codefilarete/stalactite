package org.codefilarete.stalactite.engine.runtime.load;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class AbstractJoinNodeTest {
	
	@Test
	void addingANodeToAnotherOne_treeOfNewNodeIsTheSameAsTheOneItIsAttachedTo() {
		EntityJoinTree<Object, Object> tree = new EntityJoinTree<>(mock(EntityInflater.class), new Table<>("toto"));
		assertThat(tree.getRoot().getTree()).isSameAs(tree);
		
		JoinLink rightJoinLinkMock1 = mock(JoinLink.class);
		when(rightJoinLinkMock1.getOwner()).thenReturn(new Table<>("tata"));
		RelationJoinNode<Object, Table, Table, Object, Object> node1 = new RelationJoinNode<>(tree.getRoot(), mock(Accessor.class), mock(JoinLink.class), rightJoinLinkMock1, null, null, null, null, null, null);
		assertThat(node1.getTree()).isSameAs(tree);
		
		JoinLink rightJoinLinkMock2 = mock(JoinLink.class);
		when(rightJoinLinkMock2.getOwner()).thenReturn(new Table<>("titi"));
		RelationJoinNode<Object, Table, Table, Object, Object> node2 = new RelationJoinNode<>(node1, mock(Accessor.class) , mock(JoinLink.class), rightJoinLinkMock2, null, null, null, null, null, null);
		assertThat(node2.getTree()).isSameAs(tree);
		
	}
}
