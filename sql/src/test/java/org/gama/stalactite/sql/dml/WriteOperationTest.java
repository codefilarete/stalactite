package org.gama.stalactite.sql.dml;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.spi.LoggingEvent;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.lang.trace.ModifiableInt;
import org.gama.stalactite.sql.SimpleConnectionProvider;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.gama.stalactite.sql.dml.SQLOperation.SQLOperationListener;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class WriteOperationTest {
	
	@Test
	void execute_preparedSQL() throws SQLException {
		Connection connectionMock = mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);

		Map<Integer, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put(1, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put(2, DefaultParameterBinders.STRING_BINDER);
		
		WriteOperation<Integer> testInstance = new WriteOperation<>(
				new PreparedSQL("insert into Toto(id, name) values(?, ?)", parameterBinders),
				new SimpleConnectionProvider(connectionMock));
		testInstance.setValue(1, 1L);
		testInstance.setValue(2, "tata");
		testInstance.execute();
		verify(preparedStatementMock).setLong(1, 1L);
		verify(preparedStatementMock).setString(2, "tata");
		verify(preparedStatementMock).executeUpdate();
	}
	
	@Test
	void executeBatch_preparedSQL() throws SQLException {
		Connection connectionMock = mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);
		List<Integer> batchRowCounter = new ArrayList<>();
		doAnswer(invocation -> {
			batchRowCounter.add(1);
			return null;
		}).when(preparedStatementMock).addBatch();
		doAnswer(invocation -> {
			int[] batchCount = Arrays.toPrimitive(batchRowCounter.toArray(new Integer[0]));
			batchRowCounter.clear();
			return batchCount;
		}).when(preparedStatementMock).executeBatch();
		
		Map<Integer, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put(1, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put(2, DefaultParameterBinders.STRING_BINDER);
		
		WriteOperation<Integer> testInstance = new WriteOperation<>(
				new PreparedSQL("insert into Toto(id, name) values(?, ?)", parameterBinders),
				new SimpleConnectionProvider(connectionMock));
		testInstance.addBatch(Maps.asMap(1, (Object) 1L).add(2, "Tata"));
		int executeMultiple = testInstance.executeBatch();

		verify(preparedStatementMock).setLong(1, 1L);
		verify(preparedStatementMock).setString(2, "Tata");
		verify(preparedStatementMock).executeBatch();
		assertThat(executeMultiple).isEqualTo(1);

		clearInvocations(preparedStatementMock);
		
		testInstance.addBatch(Maps.asMap(1, (Object) 2L).add(2, "Tata"));
		testInstance.addBatch(Maps.asMap(1, (Object) 3L).add(2, "Toto"));
		executeMultiple = testInstance.executeBatch();

		verify(preparedStatementMock).setLong(1, 2L);
		verify(preparedStatementMock).setLong(1, 3L);
		verify(preparedStatementMock).setString(2, "Tata");
		verify(preparedStatementMock).setString(2, "Toto");
		verify(preparedStatementMock).executeBatch();

		assertThat(executeMultiple).isEqualTo(2);
	}
	
	@Test
	void execute_parameterizedSQL() throws SQLException {
		Connection connectionMock = mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);
		List<Integer> batchRowCounter = new ArrayList<>();
		doAnswer(invocation -> {
			batchRowCounter.add(1);
			return null;
		}).when(preparedStatementMock).addBatch();
		doAnswer(invocation -> {
			int[] batchCount = Arrays.toPrimitive(batchRowCounter.toArray(new Integer[0]));
			batchRowCounter.clear();
			return batchCount;
		}).when(preparedStatementMock).executeBatch();
		doAnswer(invocation -> 1).when(preparedStatementMock).executeUpdate();


		Map<String, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put("id", DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put("name", DefaultParameterBinders.STRING_BINDER);
		
		WriteOperation<String> testInstance = new WriteOperation<>(
				new StringParamedSQL("insert into Toto(id, name) values(:id, :name)", parameterBinders),
				new SimpleConnectionProvider(connectionMock));
		testInstance.setValue("id", 1L);
		testInstance.setValue("name", "tata");
		int executeOne = testInstance.execute();

		verify(preparedStatementMock).setLong(1, 1L);
		verify(preparedStatementMock).setString(2, "tata");
		verify(preparedStatementMock).executeUpdate();
		assertThat(executeOne).isEqualTo(1);

		clearInvocations(preparedStatementMock);
		
		parameterBinders.clear();
		parameterBinders.put("ids", DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		WriteOperation<String> testInstanceForDelete = new WriteOperation<>(
				new StringParamedSQL("delete from Toto where id in (:ids)", parameterBinders),
				new SimpleConnectionProvider(connectionMock));
		testInstanceForDelete.addBatch(Maps.asMap("ids", Arrays.asList(1L, 2L, 3L)));
		int executeMultiple = testInstanceForDelete.executeBatch();
		verify(preparedStatementMock).setLong(1, 1L);
		verify(preparedStatementMock).setLong(2, 2L);
		verify(preparedStatementMock).setLong(3, 3L);
		verify(preparedStatementMock).executeBatch();
		assertThat(executeMultiple).isEqualTo(1);
	}
	
	@Test
	void executeBatch_parameterizedSQL() throws SQLException {
		Connection connectionMock = mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);
		List<Integer> batchRowCounter = new ArrayList<>();
		doAnswer(invocation -> {
			batchRowCounter.add(1);
			return null;
		}).when(preparedStatementMock).addBatch();
		doAnswer(invocation -> {
			int[] batchCount = Arrays.toPrimitive(batchRowCounter.toArray(new Integer[0]));
			batchRowCounter.clear();
			return batchCount;
		}).when(preparedStatementMock).executeBatch();
		
		Map<String, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put("id", DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put("name", DefaultParameterBinders.STRING_BINDER);
		
		WriteOperation<String> testInstance = new WriteOperation<>(
				new StringParamedSQL("insert into Toto(id, name) values(:id, :name)", parameterBinders),
				new SimpleConnectionProvider(connectionMock));
		testInstance.addBatch(Maps.asMap("id", (Object) 1L).add("name", "Tata"));
		int executeMultiple = testInstance.executeBatch();
		verify(preparedStatementMock).setLong(1, 1L);
		verify(preparedStatementMock).setString(2, "Tata");
		verify(preparedStatementMock).executeBatch();
		assertThat(executeMultiple).isEqualTo(1);
		
		clearInvocations(preparedStatementMock);
		
		testInstance.addBatch(Maps.asMap("id", (Object) 2L).add("name", "Tata"));
		testInstance.addBatch(Maps.asMap("id", (Object) 3L).add("name", "Toto"));
		executeMultiple = testInstance.executeBatch();
		verify(preparedStatementMock).setLong(1, 2L);
		verify(preparedStatementMock).setLong(1, 3L);
		verify(preparedStatementMock).setString(2, "Tata");
		verify(preparedStatementMock).setString(2, "Toto");
		verify(preparedStatementMock).executeBatch();
		assertThat(executeMultiple).isEqualTo(2);
	}
	
	@Test
	void execute_batchLog_indexedParameters() throws SQLException {
		Connection connectionMock = mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);
		List<Integer> batchRowCounter = new ArrayList<>();
		doAnswer(invocation -> {
			batchRowCounter.add(1);
			return null;
		}).when(preparedStatementMock).addBatch();
		doAnswer(invocation -> {
			int[] batchCount = Arrays.toPrimitive(batchRowCounter.toArray(new Integer[0]));
			batchRowCounter.clear();
			return batchCount;
		}).when(preparedStatementMock).executeBatch();
		
		Map<Integer, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put(1, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put(2, DefaultParameterBinders.STRING_BINDER);

		// preparing log system to capture logs
		StringWriter logsRecipient = new StringWriter();
		Logger logger = LogManager.getLogger(SQLOperation.class);
		Level currentLevel = logger.getLevel();
		logger.setLevel(Level.TRACE);
		Layout layout = new TestLayout();
		logger.addAppender(new WriterAppender(layout, logsRecipient));
		
		WriteOperation<Integer> testInstance = new WriteOperation<>(
				new PreparedSQL("insert into Toto(id, name) values(?, ?)", parameterBinders),
				new SimpleConnectionProvider(connectionMock));
		testInstance.addBatch(Maps.asMap(1, (Object) 1L).add(2, "tata"));
		testInstance.addBatch(Maps.asMap(1, (Object) 2L).add(2, "toto"));
		
		int writeCount = testInstance.executeBatch();
		verify(preparedStatementMock).setLong(1, 1L);
		verify(preparedStatementMock).setLong(1, 2L);
		verify(preparedStatementMock).setString(2, "tata");
		verify(preparedStatementMock).setString(2, "toto");
		verify(preparedStatementMock).executeBatch();
		assertThat(writeCount).isEqualTo(2);
		
		String expectedLog = layout.format(new LoggingEvent("toto", logger, Level.DEBUG,
				"insert into Toto(id, name) values(?, ?) | {1={1=1, 2=tata}, 2={1=2, 2=toto}}", null));
		assertThat(logsRecipient.toString().contains(expectedLog)).isTrue();
		// back to normal
		logger.setLevel(currentLevel);
	}
	
	@Test
	void execute_batchLog_namedParameters() throws SQLException {
		Connection connectionMock = mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);
		List<Integer> batchRowCounter = new ArrayList<>();
		doAnswer(invocation -> {
			batchRowCounter.add(1);
			return null;
		}).when(preparedStatementMock).addBatch();
		doAnswer(invocation -> {
			int[] batchCount = Arrays.toPrimitive(batchRowCounter.toArray(new Integer[0]));
			batchRowCounter.clear();
			return batchCount;
		}).when(preparedStatementMock).executeBatch();
		
		Map<String, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put("id", DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put("name", DefaultParameterBinders.STRING_BINDER);
		
		// preparing log system to capture logs
		StringWriter logsRecipient = new StringWriter();
		Logger logger = LogManager.getLogger(SQLOperation.class);
		Level currentLevel = logger.getLevel();
		logger.setLevel(Level.TRACE);
		Layout layout = new TestLayout();
		logger.addAppender(new WriterAppender(layout, logsRecipient));
		
		WriteOperation<String> testInstance = new WriteOperation<>(
				new StringParamedSQL("insert into Toto(id, name) values(:id, :name)", parameterBinders),
				new SimpleConnectionProvider(connectionMock));
		testInstance.addBatch(Maps.asMap("id", (Object) 2L).add("name", "Tata"));
		testInstance.addBatch(Maps.asMap("id", (Object) 3L).add("name", "Toto"));
		
		int writeCount = testInstance.executeBatch();
		verify(preparedStatementMock).setLong(1, 2L);
		verify(preparedStatementMock).setLong(1, 3L);
		verify(preparedStatementMock).setString(2, "Tata");
		verify(preparedStatementMock).setString(2, "Toto");
		verify(preparedStatementMock).executeBatch();
		assertThat(writeCount).isEqualTo(2);
		
		String expectedLog = layout.format(new LoggingEvent("toto", logger, Level.DEBUG,
				"insert into Toto(id, name) values(:id, :name) | {1={name=Tata, id=2}, 2={name=Toto, id=3}}", null));
		assertThat(logsRecipient.toString().contains(expectedLog)).isTrue();
		// back to normal
		logger.setLevel(currentLevel);
	}
	
	@Test
	void execute_sensibleValuesAreNotLogged() throws SQLException {
		Connection connectionMock = mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);
		doAnswer(invocation -> 1).when(preparedStatementMock).executeUpdate();
		
		Map<Integer, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put(1, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put(2, DefaultParameterBinders.STRING_BINDER);
		
		// preparing log system to capture logs
		StringWriter logsRecipient = new StringWriter();
		Logger logger = LogManager.getLogger(SQLOperation.class);
		Level currentLevel = logger.getLevel();
		logger.setLevel(Level.TRACE);
		Layout layout = new TestLayout();
		logger.addAppender(new WriterAppender(layout, logsRecipient));
		
		WriteOperation<Integer> testInstance = new WriteOperation<>(
				new PreparedSQL("insert into Toto(id, name) values(?, ?)", parameterBinders),
				new SimpleConnectionProvider(connectionMock));
		testInstance.setNotLoggedParams(Arrays.asSet(2));
		testInstance.setValue(1, 1L);
		testInstance.setValue(2, "tata");
		int writeCount = testInstance.execute();
		
		verify(preparedStatementMock).setLong(1, 1L);
		verify(preparedStatementMock).setString(2, "tata");
		verify(preparedStatementMock).executeUpdate();
		assertThat(writeCount).isEqualTo(1);
		
		String expectedLog = layout.format(new LoggingEvent("toto", logger, Level.DEBUG,
				"insert into Toto(id, name) values(?, ?) | {1=1, 2=X-masked value-X}", null));
		assertThat(logsRecipient.toString().contains(expectedLog)).isTrue();
		// back to normal
		logger.setLevel(currentLevel);
	}
	
	@Test
	void execute_sensibleValuesAreNotLogged_batchNamedParameters() throws SQLException {
		Connection connectionMock = mock(Connection.class);
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);
		List<Integer> batchRowCounter = new ArrayList<>();
		doAnswer(invocation -> {
			batchRowCounter.add(1);
			return null;
		}).when(preparedStatementMock).addBatch();
		doAnswer(invocation -> {
			int[] batchCount = Arrays.toPrimitive(batchRowCounter.toArray(new Integer[0]));
			batchRowCounter.clear();
			return batchCount;
		}).when(preparedStatementMock).executeBatch();
		
		Map<String, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put("id", DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put("name", DefaultParameterBinders.STRING_BINDER);
		
		// preparing log system to capture logs
		StringWriter logsRecipient = new StringWriter();
		Logger logger = LogManager.getLogger(SQLOperation.class);
		Level currentLevel = logger.getLevel();
		logger.setLevel(Level.TRACE);
		Layout layout = new TestLayout();
		logger.addAppender(new WriterAppender(layout, logsRecipient));
		
		WriteOperation<String> testInstance = new WriteOperation<>(
				new StringParamedSQL("insert into Toto(id, name) values(:id, :name)", parameterBinders),
				new SimpleConnectionProvider(connectionMock));
		testInstance.setNotLoggedParams(Arrays.asSet("name"));
		testInstance.addBatch(Maps.asMap("id", (Object) 2L).add("name", "Tata"));
		testInstance.addBatch(Maps.asMap("id", (Object) 3L).add("name", "Toto"));
		int writeCount = testInstance.executeBatch();

		verify(preparedStatementMock).setLong(1, 2L);
		verify(preparedStatementMock).setLong(1, 3L);
		verify(preparedStatementMock).setString(2, "Tata");
		verify(preparedStatementMock).setString(2, "Toto");
		verify(preparedStatementMock).executeBatch();
		assertThat(writeCount).isEqualTo(2);
		
		String expectedLog = layout.format(new LoggingEvent("toto", logger, Level.DEBUG,
				"insert into Toto(id, name) values(:id, :name) | {1={name=X-masked value-X, id=2}, 2={name=X-masked value-X, id=3}}", null));
		assertThat(logsRecipient.toString().contains(expectedLog)).isTrue();
		// back to normal
		logger.setLevel(currentLevel);
	}
	
	@Test
	void setValue_listenerIsCalled() throws SQLException {
		Connection connectionMock = mock(Connection.class);
		when(connectionMock.prepareStatement(anyString())).thenReturn(mock(PreparedStatement.class));
		
		Map<Integer, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put(1, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put(2, DefaultParameterBinders.STRING_BINDER);
		
		ModifiableInt beforeValuesSetInvokationCount = new ModifiableInt();
		Map<Integer, Object> capturedValues = new HashMap<>(); 
		WriteOperation<Integer> testInstance = new WriteOperation<>(
				new PreparedSQL("insert into Toto(id, name) values(?, ?)", parameterBinders),
				new SimpleConnectionProvider(connectionMock));
		testInstance.setListener(new SQLOperationListener<Integer>() {
			@Override
			public void onValuesSet(Map<Integer, ?> values) {
				beforeValuesSetInvokationCount.increment();
			}
			
			@Override
			public void onValueSet(Integer param, Object value) {
				capturedValues.put(param, value);
			}
		});
		
		testInstance.setValue(1, 1L);
		testInstance.setValue(2, "tata");
		assertThat(beforeValuesSetInvokationCount.getValue()).isEqualTo(0);
		assertThat(capturedValues).isEqualTo(Maps.asHashMap(1, (Object) 1L).add(2, "tata"));
	}
	
	
	
	private static class TestLayout extends Layout {
		
		private final StringAppender appender = new StringAppender();
		
		@Override
		public String format(LoggingEvent event) {
			appender.cutTail(appender.length());
			appender.ccat(event.getLevel(), event.getLoggerName(), event.getRenderedMessage(), LINE_SEP, "\t");
			return appender.toString();
		}
		
		@Override
		public boolean ignoresThrowable() {
			return false;
		}
		
		@Override
		public void activateOptions() {
			
		}
	}
}