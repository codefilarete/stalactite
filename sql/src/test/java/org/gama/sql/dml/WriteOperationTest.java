package org.gama.sql.dml;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
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
import org.gama.sql.ConnectionProvider;
import org.gama.sql.SimpleConnectionProvider;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Guillaume Mary
 */
public class WriteOperationTest {
	
	private ConnectionProvider connectionProvider;
	
	@Before
	public void setUp() throws SQLException {
		// Connection provider
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		Connection connection = hsqldbInMemoryDataSource.getConnection();
		Statement statement = connection.createStatement();
		statement.execute("create table Toto(id bigint, name varchar(50))");
		
		connectionProvider = new SimpleConnectionProvider(connection);
	}
	
	@Test
	public void testExecute_preparedSQL() {
		Map<Integer, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put(1, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put(2, DefaultParameterBinders.STRING_BINDER);
		
		WriteOperation<Integer> testInstance = new WriteOperation<>(new PreparedSQL("insert into Toto(id, name) values(?, ?)", parameterBinders), connectionProvider);
		testInstance.setValue(1, 1L);
		testInstance.setValue(2, "tata");
		int executeOne = testInstance.execute();
		assertEquals(1, executeOne);
	}
	
	@Test
	public void testExecuteBatch_preparedSQL() {
		Map<Integer, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put(1, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put(2, DefaultParameterBinders.STRING_BINDER);
		
		WriteOperation<Integer> testInstance = new WriteOperation<>(new PreparedSQL("insert into Toto(id, name) values(?, ?)", parameterBinders), connectionProvider);
		testInstance.addBatch(Maps.asMap(1, (Object) 1L).add(2, "Tata"));
		int executeMultiple = testInstance.executeBatch();
		assertEquals(1, executeMultiple);
		testInstance.addBatch(Maps.asMap(1, (Object) 2L).add(2, "Tata"));
		testInstance.addBatch(Maps.asMap(1, (Object) 3L).add(2, "Tata"));
		executeMultiple = testInstance.executeBatch();
		assertEquals(2, executeMultiple);
	}
	
	@Test
	public void testExecute_parameterizedSQL() {
		Map<String, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put("id", DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put("name", DefaultParameterBinders.STRING_BINDER);
		
		WriteOperation<String> testInstance = new WriteOperation<>(new StringParamedSQL("insert into Toto(id, name) values(:id, :name)", parameterBinders), connectionProvider);
		testInstance.setValue("id", 1L);
		testInstance.setValue("name", "tata");
		int executeOne = testInstance.execute();
		assertEquals(1, executeOne);
		
		parameterBinders.clear();
		parameterBinders.put("ids", DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		WriteOperation<String> testInstanceForDelete = new WriteOperation<>(new StringParamedSQL("delete from Toto where id in (:ids)", parameterBinders), connectionProvider);
		testInstanceForDelete.addBatch(Maps.asMap("ids", Arrays.asList(1L, 2L, 3L)));
		int executeMultiple = testInstanceForDelete.executeBatch();
		assertEquals(1, executeMultiple);
	}
	
	@Test
	public void testExecuteBatch_parameterizedSQL() {
		Map<String, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put("id", DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put("name", DefaultParameterBinders.STRING_BINDER);
		
		WriteOperation<String> testInstance = new WriteOperation<>(new StringParamedSQL("insert into Toto(id, name) values(:id, :name)", parameterBinders), connectionProvider);
		testInstance.addBatch(Maps.asMap("id", (Object) 1L).add("name", "Tata"));
		int executeMultiple = testInstance.executeBatch();
		assertEquals(1, executeMultiple);
		testInstance.addBatch(Maps.asMap("id", (Object) 2L).add("name", "Tata"));
		testInstance.addBatch(Maps.asMap("id", (Object) 3L).add("name", "Toto"));
		executeMultiple = testInstance.executeBatch();
		assertEquals(2, executeMultiple);
	}
	
	@Test
	public void testExecute_batchLog_indexedParameters() {
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
		
		WriteOperation<Integer> testInstance = new WriteOperation<>(new PreparedSQL("insert into Toto(id, name) values(?, ?)", parameterBinders), connectionProvider);
		testInstance.addBatch(Maps.asMap(1, (Object) 1L).add(2, "tata"));
		testInstance.addBatch(Maps.asMap(1, (Object) 2L).add(2, "toto"));
		
		int writeCount = testInstance.executeBatch();
		assertEquals(2, writeCount);
		
		String expectedLog = layout.format(new LoggingEvent("toto", logger, Level.DEBUG,
				"insert into Toto(id, name) values(?, ?) | {1={1=1, 2=tata}, 2={1=2, 2=toto}}", null));
		assertTrue(logsRecipient.toString().contains(expectedLog));
		// back to normal
		logger.setLevel(currentLevel);
	}
	
	@Test
	public void testExecute_batchLog_namedParameters() {
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
		
		WriteOperation<String> testInstance = new WriteOperation<>(new StringParamedSQL("insert into Toto(id, name) values(:id, :name)", parameterBinders), connectionProvider);
		testInstance.addBatch(Maps.asMap("id", (Object) 2L).add("name", "Tata"));
		testInstance.addBatch(Maps.asMap("id", (Object) 3L).add("name", "Toto"));
		
		int writeCount = testInstance.executeBatch();
		assertEquals(2, writeCount);
		
		String expectedLog = layout.format(new LoggingEvent("toto", logger, Level.DEBUG,
				"insert into Toto(id, name) values(:id, :name) | {1={name=Tata, id=2}, 2={name=Toto, id=3}}", null));
		assertTrue(logsRecipient.toString().contains(expectedLog));
		// back to normal
		logger.setLevel(currentLevel);
	}
	
	@Test
	public void testExecute_sensibleValuesAreNotLogged() {
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
		
		WriteOperation<Integer> testInstance = new WriteOperation<>(new PreparedSQL("insert into Toto(id, name) values(?, ?)", parameterBinders), connectionProvider);
		testInstance.setNotLoggedParams(Arrays.asSet(2));
		testInstance.setValue(1, 1L);
		testInstance.setValue(2, "tata");
		
		int writeCount = testInstance.execute();
		assertEquals(1, writeCount);
		
		String expectedLog = layout.format(new LoggingEvent("toto", logger, Level.DEBUG,
				"insert into Toto(id, name) values(?, ?) | {1=1, 2=X-masked value-X}", null));
		assertTrue(logsRecipient.toString().contains(expectedLog));
		// back to normal
		logger.setLevel(currentLevel);
	}
	
	@Test
	public void testExecute_sensibleValuesAreNotLogged_batchNamedParameters() {
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
		
		WriteOperation<String> testInstance = new WriteOperation<>(new StringParamedSQL("insert into Toto(id, name) values(:id, :name)", parameterBinders), connectionProvider);
		testInstance.setNotLoggedParams(Arrays.asSet("name"));
		testInstance.addBatch(Maps.asMap("id", (Object) 2L).add("name", "Tata"));
		testInstance.addBatch(Maps.asMap("id", (Object) 3L).add("name", "Toto"));
		
		int writeCount = testInstance.executeBatch();
		assertEquals(2, writeCount);
		
		String expectedLog = layout.format(new LoggingEvent("toto", logger, Level.DEBUG,
				"insert into Toto(id, name) values(:id, :name) | {1={name=X-masked value-X, id=2}, 2={name=X-masked value-X, id=3}}", null));
		assertTrue(logsRecipient.toString().contains(expectedLog));
		// back to normal
		logger.setLevel(currentLevel);
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