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
	void giveConnection_2ThreadsAskForAConnection_2DifferentOnesAreGiven() throws SQLException, InterruptedException {
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

}