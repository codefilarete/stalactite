package org.codefilarete.stalactite.spring.transaction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import org.assertj.core.api.Assertions;
import org.codefilarete.tool.trace.ModifiableInt;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * @author Guillaume Mary
 */
class JpaConnectionProviderTest {
	
	@Test
	void giveConnection_givenDataSourceTransactionManager_itsDataSourceIsCalled() throws SQLException {
		ModifiableInt getDataSourceInvokationCounter = new ModifiableInt();
		DataSource dataSourceMock = Mockito.mock(DataSource.class);
		Connection expectedConnection = Mockito.mock(Connection.class);
		Mockito.when(dataSourceMock.getConnection()).thenReturn(expectedConnection);
		JpaConnectionProvider testInstance = new JpaConnectionProvider(new JpaTransactionManager() {
			@Override
			public DataSource getDataSource() {
				getDataSourceInvokationCounter.increment();
				return dataSourceMock;
			}
		});
		
		// we simulate Spring behavior on transaction synchronization
		TransactionSynchronizationManager.setActualTransactionActive(true);
		TransactionSynchronizationManager.bindResource(dataSourceMock, new ConnectionHolder(expectedConnection));
		
		// When
		Connection actualConnection = testInstance.giveConnection();
		// Then
		Assertions.assertThat(getDataSourceInvokationCounter.getValue()).isEqualTo(1);
		Assertions.assertThat(actualConnection).isSameAs(expectedConnection);
		
		// cleaning context for next tests
		TransactionSynchronizationManager.setActualTransactionActive(false);
		TransactionSynchronizationManager.unbindResource(dataSourceMock);
	}
	
}