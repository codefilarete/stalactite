package org.codefilarete.stalactite.sql.spring.transaction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.stalactite.engine.SeparateTransactionExecutor.JdbcOperation;
import org.codefilarete.stalactite.sql.spring.transaction.PlatformTransactionManagerConnectionProvider;
import org.codefilarete.stalactite.sql.spring.transaction.TransactionManagerDataSourceProvider;
import org.codefilarete.tool.exception.Exceptions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.internal.stubbing.answers.ThrowsException;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class PlatformTransactionManagerConnectionProviderTest {
	
	@Test
	void executeInNewTransaction_whenOperationSucceeds_commitIsInvoked() {
		PlatformTransactionManager transactionManagerMock = mock(PlatformTransactionManager.class);
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(transactionManagerMock, mock(TransactionManagerDataSourceProvider.class));
		
		// When
		JdbcOperation jdbcOperationMock = mock(JdbcOperation.class);
		testInstance.executeInNewTransaction(jdbcOperationMock);
		
		// Then
		InOrder inOrder = inOrder(jdbcOperationMock, transactionManagerMock);
		// action is invoked
		inOrder.verify(jdbcOperationMock).execute();
		// transaction is committed
		inOrder.verify(transactionManagerMock).commit(any());
	}
	
	@Test
	void executeInNewTransaction_whenOperationFails_rollbackIsInvoked() {
		PlatformTransactionManager transactionManagerMock = mock(PlatformTransactionManager.class);
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(transactionManagerMock, mock(TransactionManagerDataSourceProvider.class));
		
		// When
		JdbcOperation jdbcOperationMock = mock(JdbcOperation.class);
		doAnswer(new ThrowsException(new RuntimeException())).when(jdbcOperationMock).execute();
		
		assertThatThrownBy(() -> testInstance.executeInNewTransaction(jdbcOperationMock))
				.extracting(t -> Exceptions.findExceptionInCauses(t, RuntimeException.class))
				.isNotNull();
		
		// Then
		InOrder inOrder = inOrder(jdbcOperationMock, transactionManagerMock);
		// action is invoked
		inOrder.verify(jdbcOperationMock).execute();
		// transaction is rolled back
		inOrder.verify(transactionManagerMock).rollback(any());
	}
	
	@Test
	void giveConnection_returnsActiveTransactionConnection() throws SQLException {
		DataSource dataSourceMock = mock(DataSource.class);
		Connection expectedConnection = mock(Connection.class);
		when(dataSourceMock.getConnection()).thenReturn(expectedConnection);
		// we simulate Spring behavior on transaction synchronization
		TransactionSynchronizationManager.setActualTransactionActive(true);
		TransactionSynchronizationManager.bindResource(dataSourceMock, new ConnectionHolder(expectedConnection));
		
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(new DataSourceTransactionManager(dataSourceMock), () -> dataSourceMock);
		Connection currentConnection = testInstance.giveConnection();
		assertThat(currentConnection).isSameAs(expectedConnection);
	}
	
	@Test
	void giveConnection_transactionIsNoMoreActive_throwsException() throws SQLException {
		DataSource dataSourceMock = mock(DataSource.class);
		Connection expectedConnection = mock(Connection.class);
		when(dataSourceMock.getConnection()).thenReturn(expectedConnection);
		
		// bounding resource but declaring transaction as not active
		TransactionSynchronizationManager.setActualTransactionActive(false);
		TransactionSynchronizationManager.bindResource(dataSourceMock, new ConnectionHolder(expectedConnection));
		
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(new DataSourceTransactionManager(dataSourceMock), () -> dataSourceMock);
		assertThatThrownBy(testInstance::giveConnection)
			.extracting(t -> Exceptions.findExceptionInCauses(t, IllegalStateException.class), InstanceOfAssertFactories.THROWABLE)
			.hasMessage("No active transaction");
	}
	
	@Test
	void giveConnection_noActiveTransaction_throwsException() {
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(new DataSourceTransactionManager(), () -> null);
		assertThatThrownBy(testInstance::giveConnection)
			.extracting(t -> Exceptions.findExceptionInCauses(t, IllegalStateException.class), InstanceOfAssertFactories.THROWABLE)
			.hasMessage("No active transaction");
	}
}