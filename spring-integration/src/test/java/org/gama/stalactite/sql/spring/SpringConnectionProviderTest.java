package org.gama.stalactite.sql.spring;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.tool.exception.Exceptions;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class SpringConnectionProviderTest {
	
	@Test
	void giveConnection_returnsActiveTransactionConnection() throws SQLException {
		DataSource dataSourceMock = mock(DataSource.class);
		Connection expectedConnection = mock(Connection.class);
		when(dataSourceMock.getConnection()).thenReturn(expectedConnection);
		// we simulate Spring behavior on transaction syncronization
		TransactionSynchronizationManager.setActualTransactionActive(true);
		TransactionSynchronizationManager.bindResource(dataSourceMock, new ConnectionHolder(expectedConnection));
		
		SpringConnectionProvider testInstance = new SpringConnectionProvider(new DataSourceTransactionManager(dataSourceMock));
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
		
		SpringConnectionProvider testInstance = new SpringConnectionProvider(new DataSourceTransactionManager(dataSourceMock));
		assertThatThrownBy(testInstance::giveConnection)
				.extracting(t -> Exceptions.findExceptionInCauses(t, IllegalStateException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("No active transaction");
	}
	
	@Test
	void giveConnection_noActiveTransaction_throwsException() {
		SpringConnectionProvider testInstance = new SpringConnectionProvider(new DataSourceTransactionManager());
		assertThatThrownBy(testInstance::giveConnection)
				.extracting(t -> Exceptions.findExceptionInCauses(t, IllegalStateException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("No active transaction");
	}
}