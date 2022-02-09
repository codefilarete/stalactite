package org.codefilarete.stalactite.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;

/**
 * @author Guillaume Mary
 */
class TransactionAwareConnectionProviderTest {
	
	@Test
	void commitIsCalled_commitMethodsAreInvoked() throws SQLException {
		Connection connection = Mockito.mock(Connection.class);
		TransactionAwareConnectionProvider testInstance = new TransactionAwareConnectionProvider(new SimpleConnectionProvider(connection));
		
		CommitListener commitListenerMock = Mockito.mock(CommitListener.class);
		testInstance.addCommitListener(commitListenerMock);
		
		testInstance.giveConnection().commit();
		Mockito.verify(commitListenerMock).beforeCommit();
		Mockito.verify(commitListenerMock).afterCommit();
	}
	
	@Test
	void rollbackIsCalled_rollbackMethodsAreInvoked() throws SQLException {
		Connection connection = Mockito.mock(Connection.class);
		TransactionAwareConnectionProvider testInstance = new TransactionAwareConnectionProvider(new SimpleConnectionProvider(connection));
		
		RollbackListener rollbackListenerMock = Mockito.mock(RollbackListener.class);
		testInstance.addRollbackListener(rollbackListenerMock);
		
		testInstance.giveConnection().rollback();
		Mockito.verify(rollbackListenerMock).beforeRollback();
		Mockito.verify(rollbackListenerMock).afterRollback();
		Mockito.verify(rollbackListenerMock, Mockito.never()).beforeRollback(any());
		Mockito.verify(rollbackListenerMock, Mockito.never()).afterRollback(any());
	}
	
	@Test
	void rollbackSavepointIsCalled_rollbackSavepointMethodsAreInvoked() throws SQLException {
		Connection connection = Mockito.mock(Connection.class);
		TransactionAwareConnectionProvider testInstance = new TransactionAwareConnectionProvider(new SimpleConnectionProvider(connection));
		
		RollbackListener rollbackListenerMock = Mockito.mock(RollbackListener.class);
		testInstance.addRollbackListener(rollbackListenerMock);
		
		Savepoint savepoint = testInstance.giveConnection().setSavepoint();
		testInstance.giveConnection().rollback(savepoint);
		Mockito.verify(rollbackListenerMock).beforeRollback(savepoint);
		Mockito.verify(rollbackListenerMock).afterRollback(savepoint);
		Mockito.verify(rollbackListenerMock, Mockito.never()).beforeRollback();
		Mockito.verify(rollbackListenerMock, Mockito.never()).afterRollback();
	}
	
}