package org.gama.sql.dml;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.sql.IConnectionProvider;
import org.gama.sql.SimpleConnectionProvider;
import org.gama.sql.binder.LongBinder;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.StringBinder;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class WriteOperationTest {
	
	private IConnectionProvider connectionProvider;
	
	@BeforeMethod
	protected void setUp() throws SQLException {
		// Connection provider
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		Connection connection = hsqldbInMemoryDataSource.getConnection();
		Statement statement = connection.createStatement();
		statement.execute("create table Toto(id bigint, name varchar(50))");
		
		connectionProvider = new SimpleConnectionProvider(connection);
	}
	
	@Test
	public void testExecute_preparedSQL() throws SQLException {
		Map<Integer, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put(1, new LongBinder());
		parameterBinders.put(2, new StringBinder());
		
		WriteOperation<Integer> testInstance = new WriteOperation<>(new PreparedSQL("insert into Toto(id, name) values(?, ?)", parameterBinders), connectionProvider);
		testInstance.setValue(1, 1L);
		testInstance.setValue(2, "tata");
		int executeOne = testInstance.execute();
		assertEquals(1, executeOne);
	}
	
	@Test
	public void testExecuteBatch_preparedSQL() throws SQLException {
		Map<Integer, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put(1, new LongBinder());
		parameterBinders.put(2, new StringBinder());
		
		WriteOperation<Integer> testInstance = new WriteOperation<>(new PreparedSQL("insert into Toto(id, name) values(?, ?)", parameterBinders), connectionProvider);
		testInstance.addBatch(Maps.asMap(1, (Object) 1L).add(2, "Tata"));
		int[] executeMultiple = testInstance.executeBatch();
		assertEquals(1, executeMultiple[0]);
		testInstance.addBatch(Maps.asMap(1, (Object) 2L).add(2, "Tata"));
		testInstance.addBatch(Maps.asMap(1, (Object) 3L).add(2, "Tata"));
		executeMultiple = testInstance.executeBatch();
		assertEquals(1, executeMultiple[0]);
		assertEquals(1, executeMultiple[1]);
	}
	
	@Test
	public void testExecute_parameterizedSQL() throws SQLException {
		Map<String, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put("id", new LongBinder());
		parameterBinders.put("name", new StringBinder());
		
		WriteOperation<String> testInstance = new WriteOperation<>(new ParameterizedSQL("insert into Toto(id, name) values(:id, :name)", parameterBinders), connectionProvider);
		testInstance.setValue("id", 1L);
		testInstance.setValue("name", "tata");
		int executeOne = testInstance.execute();
		assertEquals(1, executeOne);
		
		parameterBinders.clear();
		parameterBinders.put("ids", new LongBinder());
		WriteOperation<String> testInstanceForDelete = new WriteOperation<>(new ParameterizedSQL("delete from Toto where id in (:ids)", parameterBinders), connectionProvider);
		testInstanceForDelete.addBatch(Maps.asMap("ids", (Object) Arrays.asList(1L, 2L, 3L)));
		int[] executeMultiple = testInstanceForDelete.executeBatch();
		assertEquals(1, executeMultiple[0]);
	}
	
	@Test
	public void testExecuteBatch_parameterizedSQL() throws SQLException {
		Map<String, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put("id", new LongBinder());
		parameterBinders.put("name", new StringBinder());
		
		WriteOperation<String> testInstance = new WriteOperation<>(new ParameterizedSQL("insert into Toto(id, name) values(:id, :name)", parameterBinders), connectionProvider);
		testInstance.addBatch(Maps.asMap("id", (Object) 1L).add("name", "Tata"));
		int[] executeMultiple = testInstance.executeBatch();
		assertEquals(1, executeMultiple[0]);
		testInstance.addBatch(Maps.asMap("id", (Object) 2L).add("name", "Tata"));
		testInstance.addBatch(Maps.asMap("id", (Object) 3L).add("name", "Tata"));
		executeMultiple = testInstance.executeBatch();
		assertEquals(1, executeMultiple[0]);
		assertEquals(1, executeMultiple[1]);
	}
}