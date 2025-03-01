package org.codefilarete.stalactite.sql;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.ConnectionProvider.DataSourceConnectionProvider;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ClosedConnectionRetryerProviderTest {
	
	
	@Test
	void giveConnection_previousConnectionIsClosed_giveANewOne() throws SQLException {
		DataSource dataSourceMock = mock(DataSource.class);
		
		// Setting 4 dead connections
		when(dataSourceMock.getConnection()).thenAnswer(new CloseableConnection(4));
		// Setting 20 to retry to be sure we won't reach opening attempts
		ClosedConnectionRetryerProvider testInstance = new ClosedConnectionRetryerProvider(new DataSourceConnectionProvider(dataSourceMock), 20);
		
		Connection givenConnection = testInstance.giveConnection();
		
		assertThat(givenConnection.isClosed()).isFalse();
		// The fifth is the open one
		verify(dataSourceMock, times(5)).getConnection();
	}
	
	
	/**
	 * Class that let us return some dead connections while calling {@link DataSource#getConnection()}
	 */
	private static class CloseableConnection implements Answer<Connection> {
		
		private final int closedConnectionsCount;
		private int currentClosedConnectionsCount;
		
		private CloseableConnection(int closedConnectionsCount) {
			this.closedConnectionsCount = closedConnectionsCount;
		}
		
		@Override
		public Connection answer(InvocationOnMock invocation) throws Throwable {
			Connection connectionMock = mock(Connection.class);
			if (currentClosedConnectionsCount < closedConnectionsCount) {
				when(connectionMock.isClosed()).thenReturn(true);
				currentClosedConnectionsCount++;
			}
			return connectionMock;
		}
	}
	
}