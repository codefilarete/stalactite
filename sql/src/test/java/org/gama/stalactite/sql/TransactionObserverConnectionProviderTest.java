package org.gama.stalactite.sql;

import java.sql.SQLException;
import java.sql.Savepoint;

import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;

/**
 * @author Guillaume Mary
 */
public class TransactionObserverConnectionProviderTest {
	
	@Test
	public void test_whenCommitIsCalled_commitMethodsAreInvoked() throws SQLException {
		HSQLDBInMemoryDataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
		TransactionObserverConnectionProvider testInstance = new TransactionObserverConnectionProvider(new SimpleConnectionProvider(inMemoryDataSource.getConnection()));
		
		CommitListener commitListenerMock = Mockito.mock(CommitListener.class);
		testInstance.addCommitListener(commitListenerMock);
		
		testInstance.getCurrentConnection().commit();
		Mockito.verify(commitListenerMock).beforeCommit();
		Mockito.verify(commitListenerMock).afterCommit();
	}
	
	@Test
	public void test_whenRollbackIsCalled_rollbackMethodsAreInvoked() throws SQLException {
		HSQLDBInMemoryDataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
		TransactionObserverConnectionProvider testInstance = new TransactionObserverConnectionProvider(new SimpleConnectionProvider(inMemoryDataSource.getConnection()));
		
		RollbackListener rollbackListenerMock = Mockito.mock(RollbackListener.class);
		testInstance.addRollbackListener(rollbackListenerMock);
		
		testInstance.getCurrentConnection().rollback();
		Mockito.verify(rollbackListenerMock).beforeRollback();
		Mockito.verify(rollbackListenerMock).afterRollback();
		Mockito.verify(rollbackListenerMock, Mockito.never()).beforeRollback(any());
		Mockito.verify(rollbackListenerMock, Mockito.never()).afterRollback(any());
	}
	
	@Test
	public void test_whenRollbackSavepointIsCalled_rollbackSavepointMethodsAreInvoked() throws SQLException {
		HSQLDBInMemoryDataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
		TransactionObserverConnectionProvider testInstance = new TransactionObserverConnectionProvider(new SimpleConnectionProvider(inMemoryDataSource.getConnection()));
		
		RollbackListener rollbackListenerMock = Mockito.mock(RollbackListener.class);
		testInstance.addRollbackListener(rollbackListenerMock);
		
		testInstance.getCurrentConnection().setAutoCommit(false);	// necessary (at least with HSQLDB) to activate savepoint feature
		Savepoint savepoint = testInstance.getCurrentConnection().setSavepoint();
		testInstance.getCurrentConnection().rollback(savepoint);
		Mockito.verify(rollbackListenerMock).beforeRollback(savepoint);
		Mockito.verify(rollbackListenerMock).afterRollback(savepoint);
		Mockito.verify(rollbackListenerMock, Mockito.never()).beforeRollback();
		Mockito.verify(rollbackListenerMock, Mockito.never()).afterRollback();
	}
	
}