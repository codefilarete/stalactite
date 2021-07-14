package org.gama.stalactite.sql.dml;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.gama.lang.collection.Maps;
import org.gama.lang.trace.ModifiableInt;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.SimpleConnectionProvider;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.gama.stalactite.sql.dml.SQLOperation.SQLOperationListener;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
public class ReadOperationTest {
	
	private ConnectionProvider connectionProvider;
	
	@BeforeEach
	public void setUp() throws SQLException {
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
		parameterBinders.put(1, DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put(2, DefaultParameterBinders.STRING_BINDER);
		
		ReadOperation<Integer> testInstance = new ReadOperation<>(new PreparedSQL("select count(*) from Toto where id = ? and name = ?", parameterBinders), connectionProvider);
		testInstance.setValue(1, 1L);
		testInstance.setValue(2, "tata");
		ResultSet resultSet = testInstance.execute();
		assertThat(resultSet.next()).isTrue();
		assertThat(resultSet.getInt(1)).isEqualTo(0);
	}
	
	@Test
	public void testExecute_parameterizedSQL() throws SQLException {
		Map<String, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put("id", DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put("name", DefaultParameterBinders.STRING_BINDER);
		
		ReadOperation<String> testInstance = new ReadOperation<>(new StringParamedSQL("select count(*) from Toto where id = :id and name = :name", parameterBinders), connectionProvider);
		testInstance.setValue("id", 1L);
		testInstance.setValue("name", "tata");
		ResultSet resultSet = testInstance.execute();
		assertThat(resultSet.next()).isTrue();
		assertThat(resultSet.getInt(1)).isEqualTo(0);
	}
	
	@Test
	public void testListenerIsCalled() {
		Map<String, ParameterBinder> parameterBinders = new HashMap<>();
		parameterBinders.put("id", DefaultParameterBinders.LONG_PRIMITIVE_BINDER);
		parameterBinders.put("name", DefaultParameterBinders.STRING_BINDER);
		
		ModifiableInt beforeValuesSetInvokationCount = new ModifiableInt();
		Map<String, Object> capturedValues = new HashMap<>();
		ReadOperation<String> testInstance = new ReadOperation<>(new StringParamedSQL("select count(*) from Toto where id = :id and name = :name", parameterBinders), connectionProvider);
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