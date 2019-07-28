package org.gama.stalactite.sql;

import java.sql.Savepoint;

import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Guillaume Mary
 */
class TransactionListenerTest {
	
	@Test
	void beforeCommit() {
		TransactionListener testInstance = mock(TransactionListener.class);
		doCallRealMethod().when(testInstance).beforeCommit();
		testInstance.beforeCommit();
		verify(testInstance).beforeCommit();
		verify(testInstance).beforeCompletion();
		verifyNoMoreInteractions(testInstance);
	}
	
	@Test
	void afterCommit() {
		TransactionListener testInstance = mock(TransactionListener.class);
		doCallRealMethod().when(testInstance).afterCommit();
		testInstance.afterCommit();
		verify(testInstance).afterCommit();
		verify(testInstance).afterCompletion();
		verifyNoMoreInteractions(testInstance);
	}
	
	@Test
	void beforeRollback() {
		TransactionListener testInstance = mock(TransactionListener.class);
		doCallRealMethod().when(testInstance).beforeRollback();
		testInstance.beforeRollback();
		verify(testInstance).beforeRollback();
		verify(testInstance).beforeCompletion();
		verifyNoMoreInteractions(testInstance);
	}
	
	@Test
	void afterRollback() {
		TransactionListener testInstance = mock(TransactionListener.class);
		doCallRealMethod().when(testInstance).afterRollback();
		testInstance.afterRollback();
		verify(testInstance).afterRollback();
		verify(testInstance).afterCompletion();
		verifyNoMoreInteractions(testInstance);
	}
	
	@Test
	void beforeRollback_savepoint() {
		TransactionListener testInstance = mock(TransactionListener.class);
		Savepoint savepointArg = mock(Savepoint.class);
		doCallRealMethod().when(testInstance).beforeRollback(any(Savepoint.class));
		testInstance.beforeRollback(savepointArg);
		verify(testInstance).beforeRollback(eq(savepointArg));
		verify(testInstance).beforeCompletion(eq(savepointArg));
		verifyNoMoreInteractions(testInstance);
	}
	
	@Test
	void afterRollback_savepoint() {
		TransactionListener testInstance = mock(TransactionListener.class);
		Savepoint savepointArg = mock(Savepoint.class);
		doCallRealMethod().when(testInstance).afterRollback(any(Savepoint.class));
		testInstance.afterRollback(savepointArg);
		verify(testInstance).afterRollback(eq(savepointArg));
		verify(testInstance).afterCompletion(eq(savepointArg));
		verifyNoMoreInteractions(testInstance);
	}
}