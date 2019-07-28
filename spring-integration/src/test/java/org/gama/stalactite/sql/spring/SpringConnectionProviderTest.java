package org.gama.stalactite.sql.spring;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import org.gama.lang.test.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class SpringConnectionProviderTest {
	
	@Test
	void getCurrentConnection_returnsActiveTransactionConnection() throws SQLException {
		DataSource dataSourceMock = mock(DataSource.class);
		Connection expectedConnection = mock(Connection.class);
		when(dataSourceMock.getConnection()).thenReturn(expectedConnection);
		// we simulate Spring behavior on transaction syncronization
		TransactionSynchronizationManager.setActualTransactionActive(true);
		TransactionSynchronizationManager.bindResource(dataSourceMock, new ConnectionHolder(expectedConnection));
		
		SpringConnectionProvider testInstance = new SpringConnectionProvider(new DataSourceTransactionManager(dataSourceMock));
		Connection currentConnection = testInstance.getCurrentConnection();
		Assertions.assertEquals(currentConnection, expectedConnection);
	}
	
	@Test
	void getCurrentConnection_transactionIsNoMoreActive_throwsException() throws SQLException {
		DataSource dataSourceMock = mock(DataSource.class);
		Connection expectedConnection = mock(Connection.class);
		when(dataSourceMock.getConnection()).thenReturn(expectedConnection);
		
		// bounding resource but declaring transaction as not active
		TransactionSynchronizationManager.setActualTransactionActive(false);
		TransactionSynchronizationManager.bindResource(dataSourceMock, new ConnectionHolder(expectedConnection));
		
		SpringConnectionProvider testInstance = new SpringConnectionProvider(new DataSourceTransactionManager(dataSourceMock));
		Assertions.assertThrows(testInstance::getCurrentConnection, Assertions.hasExceptionInCauses(IllegalStateException.class)
				.andProjection(Assertions.hasMessage("No active transaction")));
	}
	
	@Test
	void getCurrentConnection_noActiveTransaction_throwsException() {
		SpringConnectionProvider testInstance = new SpringConnectionProvider(new DataSourceTransactionManager());
		Assertions.assertThrows(testInstance::getCurrentConnection, Assertions.hasExceptionInCauses(IllegalStateException.class)
				.andProjection(Assertions.hasMessage("No active transaction")));
	}
}