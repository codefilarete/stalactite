package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codefilarete.stalactite.engine.runtime.OptimizedUpdatePersister;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.Maps.ChainingHashMap;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.RollbackListener;
import org.codefilarete.stalactite.sql.RollbackObserver;
import org.codefilarete.stalactite.sql.TransactionAwareConnectionProvider;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.stalactite.sql.result.InMemoryResultSet;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class OptimizedUpdatePersisterTest {
	
	@Test
	void cachingQueryConnectionProvider_implementsRollBackObserverWhenGivenOneDoes() {
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class);
		ConnectionProvider testInstance = OptimizedUpdatePersister.wrapWithQueryCache(
				new ConnectionConfigurationSupport(new TransactionAwareConnectionProvider(connectionProviderMock), 10)).getConnectionProvider();
		assertThat(testInstance instanceof RollbackObserver).isTrue();
		
		testInstance = OptimizedUpdatePersister.wrapWithQueryCache(
				new ConnectionConfigurationSupport(new CurrentThreadConnectionProvider(mock(DataSource.class)), 10)).getConnectionProvider();
		assertThat(testInstance instanceof RollbackObserver).isFalse();
	}
	
	@Test
	void cachingQueryConnectionProvider_notifiesRollbackObserverWhenGivenOneIsAwareOfTransaction() throws SQLException {
		ConnectionProvider connectionProviderMock = mock(ConnectionProvider.class);
		when(connectionProviderMock.giveConnection()).thenReturn(mock(Connection.class));
		ConnectionProvider testInstance = OptimizedUpdatePersister.wrapWithQueryCache(
				new ConnectionConfigurationSupport(new TransactionAwareConnectionProvider(connectionProviderMock), 10)).getConnectionProvider();
		
		RollbackListener rollbackListenerMock = mock(RollbackListener.class);
		
		((RollbackObserver) testInstance).addRollbackListener(rollbackListenerMock);
		testInstance.giveConnection().rollback();
		
		verify(rollbackListenerMock).beforeRollback();
	}
	
	@Test
	void cachingQueryConnectionProvider_cachesQuery() throws SQLException {
		DataSource dataSourceMock = mock(DataSource.class);
		Connection connectionMock = mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(dataSourceMock.getConnection()).thenReturn(connectionMock);
		when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);
		List<ChainingHashMap<String, String>> data = Arrays.asList(Maps.forHashMap(String.class, String.class)
				.add("a", "myValue")
				.add("b", "my second value"));
		InMemoryResultSet dummyResultSet = new InMemoryResultSet(data);
		when(preparedStatementMock.executeQuery()).thenReturn(dummyResultSet);
		ConnectionProvider testInstance = OptimizedUpdatePersister.wrapWithQueryCache(
				new ConnectionConfigurationSupport(new CurrentThreadConnectionProvider(dataSourceMock), 10)).getConnectionProvider();
		
		OptimizedUpdatePersister.QUERY_CACHE.set(new HashMap<>());
		Connection currentConnection = testInstance.giveConnection();
		PreparedStatement preparedStatement1 = currentConnection.prepareStatement("Select * from WhateverYouWant");
		ResultSet effectiveResultSet = preparedStatement1.executeQuery();
		RowIterator rsReader = new RowIterator(Maps.forHashMap(String.class, ResultSetReader.class)
				.add("a", DefaultResultSetReaders.STRING_READER)
				.add("b", DefaultResultSetReaders.STRING_READER));
		// we must read whole ResultSet to trigger cache filling
		while(effectiveResultSet.next()) {
			rsReader.convert(effectiveResultSet);
		}
		
		PreparedStatement preparedStatement2 = currentConnection.prepareStatement("Select * from WhateverYouWant");
		ResultSet cachedResultSet = preparedStatement2.executeQuery();
		List<Map<String, Object>> cachedValues = new ArrayList<>();
		while(cachedResultSet.next()) {
			cachedValues.add(rsReader.convert(effectiveResultSet).getContent());
		}
		
		// executeQuery() must have been called once thanks to cache
		Mockito.verify(preparedStatementMock).executeQuery();
		OptimizedUpdatePersister.QUERY_CACHE.remove();
		
		assertThat(data).isEqualTo(cachedValues);
	}
}