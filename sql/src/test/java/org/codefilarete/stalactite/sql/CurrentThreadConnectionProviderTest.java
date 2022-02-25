package org.codefilarete.stalactite.sql;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.sql.ConnectionWrapper;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class CurrentThreadConnectionProviderTest {
	
	@Test
	void giveConnection_connectionAutoCommitIsFalse() throws SQLException {
		DataSource dataSourceMock = mock(DataSource.class);
		Connection connectionMock = mock(Connection.class);
		when(dataSourceMock.getConnection()).thenReturn(connectionMock);
		CurrentThreadConnectionProvider testInstance = new CurrentThreadConnectionProvider(dataSourceMock);
		
		testInstance.giveConnection();
		verify(connectionMock).setAutoCommit(false);
	}
	
	@Test
	void giveConnection_callTwice_returnSameConnection() throws SQLException {
		DataSource dataSourceMock = mock(DataSource.class);
		when(dataSourceMock.getConnection()).thenReturn(mock(Connection.class));
		CurrentThreadConnectionProvider testInstance = new CurrentThreadConnectionProvider(dataSourceMock);
		
		Connection givenConnection1 = testInstance.giveConnection();
		Connection givenConnection2 = testInstance.giveConnection();
		// No superfluous DataSource.getConnection() expected
		verify(dataSourceMock, times(1)).getConnection();
		assertThat(givenConnection1).isSameAs(givenConnection2);
	}
	
	@Test
	void giveConnection_previousConnectionIsClosed_giveANewOne() throws SQLException {
		DataSource dataSourceMock = mock(DataSource.class);
		
		// Setting 4 dead connections
		when(dataSourceMock.getConnection()).thenAnswer(new CloseableConnection(4));
		// Setting 20 to retry to be sure we won't reach opening attempts
		CurrentThreadConnectionProvider testInstance = new CurrentThreadConnectionProvider(dataSourceMock, 20);
		
		Connection givenConnection = testInstance.giveConnection();
		
		assertThat(givenConnection.isClosed()).isFalse();
		// The fifth is the open one
		verify(dataSourceMock, times(5)).getConnection();
	}
	
	@Test
	void giveConnection_2ThreadsAskForAConnection_2DifferentOneAreGiven() throws SQLException, InterruptedException {
		DataSource dataSourceMock = mock(DataSource.class);
		
		when(dataSourceMock.getConnection()).thenAnswer((Answer<Connection>) invocation -> mock(Connection.class));
		CurrentThreadConnectionProvider testInstance = new CurrentThreadConnectionProvider(dataSourceMock);
		
		Connection connection = testInstance.giveConnection();
		
		Holder<Connection> connectionHolder = new Holder<>();
		Thread thread  = new Thread(() -> {
			connectionHolder.set(testInstance.giveConnection());
		});
		thread.start();
		thread.join(200);
		
		verify(dataSourceMock, times(2)).getConnection();
		assertThat(connection).isNotSameAs(connectionHolder.get());
		if (connection instanceof ConnectionWrapper) {
			assertThat(((ConnectionWrapper) connection).getDelegate()).isNotSameAs(((ConnectionWrapper) connectionHolder.get()).getDelegate());
		}
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