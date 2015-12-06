package org.gama.sql.dml;

import org.gama.sql.IConnectionProvider;
import org.gama.sql.SimpleConnectionProvider;
import org.gama.sql.binder.LongBinder;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.StringBinder;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Guillaume Mary
 */
public class ReadOperationTest {
	
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
		
		ReadOperation<Integer> testInstance = new ReadOperation<>(new PreparedSQL("select count(*) from Toto where id = ? and name = ?", parameterBinders), connectionProvider);
		testInstance.setValue(1, 1L);
		testInstance.setValue(2, "tata");
		ResultSet resultSet = testInstance.execute();
		assertTrue(resultSet.next());
		assertEquals(0, resultSet.getInt(1));
	}
	
	@Test
	public void testExecute_parameterizedSQL() throws SQLException {
		Map<String, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put("id", new LongBinder());
		parameterBinders.put("name", new StringBinder());
		
		ReadOperation<String> testInstance = new ReadOperation<>(new StringParamedSQL("select count(*) from Toto where id = :id and name = :name", parameterBinders), connectionProvider);
		testInstance.setValue("id", 1L);
		testInstance.setValue("name", "tata");
		ResultSet resultSet = testInstance.execute();
		assertTrue(resultSet.next());
		assertEquals(0, resultSet.getInt(1));
	}
}