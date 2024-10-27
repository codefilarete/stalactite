package org.codefilarete.stalactite.sql.spring.transaction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.stalactite.engine.SeparateTransactionExecutor.JdbcOperation;
import org.codefilarete.tool.exception.Exceptions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.ThrowsException;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * @author Guillaume Mary
 */
class PlatformTransactionManagerConnectionProviderTest {
	
	@Test
	void executeInNewTransaction_whenOperationSucceeds_commitIsInvoked() {
		PlatformTransactionManager transactionManagerMock = Mockito.mock(PlatformTransactionManager.class);
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(transactionManagerMock, Mockito.mock(TransactionManagerDataSourceProvider.class));
		
		// When
		JdbcOperation jdbcOperationMock = Mockito.mock(JdbcOperation.class);
		testInstance.executeInNewTransaction(jdbcOperationMock);
		
		// Then
		InOrder inOrder = Mockito.inOrder(jdbcOperationMock, transactionManagerMock);
		// action is invoked
		inOrder.verify(jdbcOperationMock).execute();
		// transaction is committed
		inOrder.verify(transactionManagerMock).commit(ArgumentMatchers.any());
	}
	
	@Test
	void executeInNewTransaction_whenOperationFails_rollbackIsInvoked() {
		PlatformTransactionManager transactionManagerMock = Mockito.mock(PlatformTransactionManager.class);
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(transactionManagerMock, Mockito.mock(TransactionManagerDataSourceProvider.class));
		
		// When
		JdbcOperation jdbcOperationMock = Mockito.mock(JdbcOperation.class);
		Mockito.doAnswer(new ThrowsException(new RuntimeException())).when(jdbcOperationMock).execute();
		
		Assertions.assertThatThrownBy(() -> testInstance.executeInNewTransaction(jdbcOperationMock))
				.extracting(t -> Exceptions.findExceptionInCauses(t, RuntimeException.class))
				.isNotNull();
		
		// Then
		InOrder inOrder = Mockito.inOrder(jdbcOperationMock, transactionManagerMock);
		// action is invoked
		inOrder.verify(jdbcOperationMock).execute();
		// transaction is rolled back
		inOrder.verify(transactionManagerMock).rollback(ArgumentMatchers.any());
	}
	
	@Test
	void giveConnection_returnsActiveTransactionConnection() throws SQLException {
		DataSource dataSourceMock = Mockito.mock(DataSource.class);
		Connection expectedConnection = Mockito.mock(Connection.class);
		Mockito.when(dataSourceMock.getConnection()).thenReturn(expectedConnection);
		// we simulate Spring behavior on transaction synchronization
		TransactionSynchronizationManager.setActualTransactionActive(true);
		TransactionSynchronizationManager.bindResource(dataSourceMock, new ConnectionHolder(expectedConnection));
		
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(new DataSourceTransactionManager(dataSourceMock), () -> dataSourceMock);
		Connection currentConnection = testInstance.giveConnection();
		Assertions.assertThat(currentConnection).isSameAs(expectedConnection);
	}
	
	@Test
	void giveConnection_transactionIsNoMoreActive_throwsException() throws SQLException {
		DataSource dataSourceMock = Mockito.mock(DataSource.class);
		Connection expectedConnection = Mockito.mock(Connection.class);
		Mockito.when(dataSourceMock.getConnection()).thenReturn(expectedConnection);
		
		// bounding resource but declaring transaction as not active
		TransactionSynchronizationManager.setActualTransactionActive(false);
		TransactionSynchronizationManager.bindResource(dataSourceMock, new ConnectionHolder(expectedConnection));
		
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(new DataSourceTransactionManager(dataSourceMock), () -> dataSourceMock);
		Assertions.assertThatThrownBy(testInstance::giveConnection)
			.extracting(t -> Exceptions.findExceptionInCauses(t, IllegalStateException.class), InstanceOfAssertFactories.THROWABLE)
			.hasMessage("No active transaction");
	}
	
	@Test
	void giveConnection_noActiveTransaction_throwsException() {
		PlatformTransactionManagerConnectionProvider testInstance = new PlatformTransactionManagerConnectionProvider(new DataSourceTransactionManager(), () -> null);
		Assertions.assertThatThrownBy(testInstance::giveConnection)
			.extracting(t -> Exceptions.findExceptionInCauses(t, IllegalStateException.class), InstanceOfAssertFactories.THROWABLE)
			.hasMessage("No active transaction");
	}
}