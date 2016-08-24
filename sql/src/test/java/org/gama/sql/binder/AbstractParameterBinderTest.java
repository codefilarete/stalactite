package org.gama.sql.binder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Collectors;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.sql.result.ResultSetIterator;
import org.gama.sql.test.DerbyInMemoryDataSource;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.sql.test.MariaDBEmbeddableDataSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class AbstractParameterBinderTest {
	
	private AbstractParameterBinder<Long> testInstance;
	
	@DataProvider
	public static Object[][] dataSources() {
		return new Object[][] {
				{ new HSQLDBInMemoryDataSource() },
				{ new DerbyInMemoryDataSource() },
				{ new MariaDBEmbeddableDataSource(3406) },
		};
	}
	
	@Before
	public void createTestInstance() {
		testInstance = new AbstractParameterBinder<Long>() {
			@Override
			public Long getNotNull(String columnName, ResultSet resultSet) throws SQLException {
				return resultSet.getLong(columnName);
			}
			
			@Override
			public void setNotNull(int valueIndex, Long value, PreparedStatement statement) throws SQLException {
				statement.setLong(valueIndex, value);
			}
		};
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testSetOnPreparedStatement(DataSource dataSource) throws SQLException {
		dataSource.getConnection().prepareStatement("create table Toto(a int)").execute();
		
		Connection connection = dataSource.getConnection();
		// Test of AbstractParameterBinder#set
		PreparedStatement statement = connection.prepareStatement("insert into Toto(a) values (?)");
		// this should not throw any exception
		testInstance.set(1, null, statement);
		statement.execute();
		// this should not throw any exception
		testInstance.set(1, 0L, statement);
		statement.execute();
		
		// Now testing that values were really inserted
		// This will also test AbstractParameterBinder#get
		ResultSet resultSet = connection.prepareStatement("select a from Toto").executeQuery();
		ResultSetIterator<Long> resultSetIterator = new ResultSetIterator<Long>(resultSet) {
			@Override
			public Long convert(ResultSet rs) throws SQLException {
				return testInstance.get("a", resultSet);
			}
		};
		assertEquals(Arrays.asSet(null, 0L), Iterables.stream(resultSetIterator).collect(Collectors.toSet()));
	}
	
}