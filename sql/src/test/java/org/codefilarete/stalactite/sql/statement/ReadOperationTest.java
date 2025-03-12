package org.codefilarete.stalactite.sql.statement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.trace.MutableInt;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class ReadOperationTest {
	
	@Test
	void execute_preparedSQL() throws SQLException {
		Connection connectionMock = mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);

		Map<Integer, ParameterBinder<?>> parameterBinders = new HashMap<>();
		parameterBinders.put(1, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put(2, DefaultParameterBinders.STRING_BINDER);
		
		ReadOperation<Integer> testInstance = new ReadOperation<>(
				new PreparedSQL("select count(*) from Toto where id = ? and name = ?", parameterBinders),
				new SimpleConnectionProvider(connectionMock));
		testInstance.setValue(1, 1L);
		testInstance.setValue(2, "tata");
		testInstance.execute();
		verify(preparedStatementMock).setLong(1, 1L);
		verify(preparedStatementMock).setString(2, "tata");
		verify(preparedStatementMock).executeQuery();
	}
	
	@Test
	void execute_parameterizedSQL() throws SQLException {
		Connection connectionMock = mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);

		Map<String, ParameterBinder<?>> parameterBinders = new HashMap<>();
		parameterBinders.put("id", DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put("name", DefaultParameterBinders.STRING_BINDER);
		
		ReadOperation<String> testInstance = new ReadOperation<>(
				new StringParamedSQL("select count(*) from Toto where id = :id and name = :name", parameterBinders),
				new SimpleConnectionProvider(connectionMock));
		testInstance.setValue("id", 1L);
		testInstance.setValue("name", "tata");
		testInstance.execute();
		verify(preparedStatementMock).setLong(1, 1L);
		verify(preparedStatementMock).setString(2, "tata");
		verify(preparedStatementMock).executeQuery();
	}
	
	@Test
	void setValue_listenerIsCalled() throws SQLException {
		Connection connectionMock = mock(Connection.class);
		when(connectionMock.prepareStatement(anyString())).thenReturn(mock(PreparedStatement.class));

		Map<String, ParameterBinder<?>> parameterBinders = new HashMap<>();
		parameterBinders.put("id", DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put("name", DefaultParameterBinders.STRING_BINDER);
		
		MutableInt beforeValuesSetInvokationCount = new MutableInt();
		Map<String, Object> capturedValues = new HashMap<>();
		ReadOperation<String> testInstance = new ReadOperation<>(
				new StringParamedSQL("select count(*) from Toto where id = :id and name = :name", parameterBinders),
				new SimpleConnectionProvider(connectionMock));
		testInstance.setListener(new SQLOperationListener<String>() {
			@Override
			public void onValuesSet(Map<String, ?> values) {
				beforeValuesSetInvokationCount.increment();
			}
			
			@Override
			public void onValueSet(String param, Object value) {
				capturedValues.put(param, value);
			}
		});
		
		testInstance.setValue("id", 1L);
		testInstance.setValue("name", "tata");
		assertThat(beforeValuesSetInvokationCount.getValue()).isEqualTo(0);
		assertThat(capturedValues).isEqualTo(Maps.asHashMap("id", (Object) 1L).add("name", "tata"));
	}
	
}