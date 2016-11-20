package org.gama.sql.binder;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.io.IOs;
import org.gama.lang.Nullable;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.sql.result.ResultSetIterator;
import org.gama.sql.test.DerbyInMemoryDataSource;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.sql.test.MariaDBEmbeddableDataSource;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class DefaultParameterBindersTest {
	
	@DataProvider
	public static Object[][] dataSources() {
		return new Object[][] {
				{ new HSQLDBInMemoryDataSource() },
				{ new DerbyInMemoryDataSource() },
				{ new MariaDBEmbeddableDataSource(3406) },
		};
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testLongBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.LONG_BINDER, dataSource, "int", Arrays.asSet(null, 42L));
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testLongPrimitiveBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER, dataSource, "int not null", Arrays.asSet(42L));
	}
	
	@Test(expected = NullPointerException.class)
	@UseDataProvider("dataSources")
	public void testLongPrimitiveBinder_nullValuePassed_NPEThrown(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER, dataSource, "int not null", Arrays.asSet(null));
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testIntegerBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.INTEGER_BINDER, dataSource, "int", Arrays.asSet(null, 42));
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testIntegerPrimitiveBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER, dataSource, "int not null", Arrays.asSet(42));
	}
	
	@Test(expected = NullPointerException.class)
	@UseDataProvider("dataSources")
	public void testIntegerPrimitiveBinder_nullValuePassed_NPEThrown(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER, dataSource, "int not null", Arrays.asSet(null));
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testDoubleBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.DOUBLE_BINDER, dataSource, "double", Arrays.asSet(null, 42.57D));
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testDoublePrimitiveBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.DOUBLE_PRIMITIVE_BINDER, dataSource, "double not null", Arrays.asSet(42.57D));
	}
	
	@Test(expected = NullPointerException.class)
	@UseDataProvider("dataSources")
	public void testDoublePrimitiveBinder_nullValuePassed_NPEThrown(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.DOUBLE_PRIMITIVE_BINDER, dataSource, "double not null", Arrays.asSet(null));
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testFloatBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.FLOAT_BINDER, dataSource, "double", Arrays.asSet(null, 42.57F));
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testFloatPrimitiveBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.FLOAT_PRIMITIVE_BINDER, dataSource, "double not null", Arrays.asSet(42.57F));
	}
	
	@Test(expected = NullPointerException.class)
	@UseDataProvider("dataSources")
	public void testFloatPrimitiveBinder_nullValuePassed_NPEThrown(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.FLOAT_PRIMITIVE_BINDER, dataSource, "double not null", Arrays.asSet(null));
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testBooleanBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.BOOLEAN_BINDER, dataSource, "boolean", Arrays.asSet(null, true, false));
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testBooleanPrimitiveBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.BOOLEAN_PRIMITIVE_BINDER, dataSource, "boolean not null", Arrays.asSet(true, false));
	}
	
	@Test(expected = NullPointerException.class)
	@UseDataProvider("dataSources")
	public void testBooleanPrimitiveBinder_nullValuePassed_NPEThrown(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.BOOLEAN_PRIMITIVE_BINDER, dataSource, "boolean not null", Arrays.asSet(null));
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testDateBinder(DataSource dataSource) throws SQLException {
		// Using Date(long) constructor leads to mistake since the Date spec (Javadoc) says : "the hours, minutes, seconds, and milliseconds to zero"  
		// So if the argument is a real millis time (like System.currentTimeMillis), the millis precision is lost by PreparedStatement.setDate()
		// (the data type is not the culprit since timestamp makes the same result), hence the comparison with the select result fails
		// Therefore we use a non-precised millis thanks to LocalDate.now()
		java.sql.Date date = java.sql.Date.valueOf(LocalDate.now());
		testParameterBinder(DefaultParameterBinders.DATE_BINDER, dataSource, "date", Arrays.asSet(null, date));
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testTimestampBinder(DataSource dataSource) throws SQLException {
		String sqlColumnType = "timestamp";
		if (dataSource instanceof MariaDBEmbeddableDataSource) {
			sqlColumnType = "timestamp(3) null"; // 3 for nano precision, else nanos are lost during storage, hence comparison fails
		}
		testParameterBinder(DefaultParameterBinders.TIMESTAMP_BINDER, dataSource, sqlColumnType, Arrays.asSet(null, new Timestamp(System.currentTimeMillis())));
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testStringBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.STRING_BINDER, dataSource, "varchar(255)", Arrays.asSet(null, "Hello world !"));
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testBinaryStreamBinder(DataSource dataSource) throws SQLException {
		InputStream inputStream = new ByteArrayInputStream("Hello world !".getBytes());
		LinkedHashSet<InputStream> valuesToInsert = Arrays.asSet(inputStream, null);
		ParameterBinder<InputStream> binarystreamBinder = DefaultParameterBinders.BINARYSTREAM_BINDER;
		// HSQLDB and Derby are already tested in their respective classes
		if (dataSource instanceof HSQLDBInMemoryDataSource) {
			binarystreamBinder = HSQLDBParameterBinders.BINARYSTREAM_BINDER;
		}
		if (dataSource instanceof DerbyInMemoryDataSource) {
			binarystreamBinder = DerbyParameterBinders.BINARYSTREAM_BINDER;
		}
		Set<InputStream> databaseContent = insertAndSelect(dataSource, binarystreamBinder, "blob", valuesToInsert);
		assertEquals(Arrays.asSet(null, "Hello world !"), convertToString(databaseContent));
	}
	
	static Set<String> convertToString(Set<InputStream> databaseContent) {
		return databaseContent.stream().map(s -> Nullable.of(s).orApply(inputStream -> {
			try(InputStream closeable = inputStream) {
				return new String(IOs.toByteArray(closeable));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}).get()).collect(Collectors.toSet());
	}
	
	private <T> void testParameterBinder(ParameterBinder<T> testInstance, DataSource dataSource, String sqlColumnType, Set<T> valuesToInsert) throws SQLException {
		Set<T> databaseContent = insertAndSelect(dataSource, testInstance, sqlColumnType, valuesToInsert);
		assertEquals(valuesToInsert, databaseContent);
	}
	
	static <T> Set<T> insertAndSelect(DataSource dataSource, ParameterBinder<T> testInstance, String sqlColumnType, Set<T> valuesToInsert) throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.prepareStatement("create table Toto(a "+sqlColumnType+")").execute();
		// Test of ParameterBinder#set
		PreparedStatement statement = connection.prepareStatement("insert into Toto(a) values (?)");
		valuesToInsert.forEach((v) -> {
			try {
				testInstance.set(1, v, statement);
				statement.execute();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
		
		// Now testing that values were really inserted
		// This will also test ParameterBinder#get
		ResultSet resultSet = connection.prepareStatement("select a from Toto").executeQuery();
		ResultSetIterator<T> resultSetIterator = new ResultSetIterator<T>(resultSet) {
			@Override
			public T convert(ResultSet rs) throws SQLException {
				return testInstance.get("a", resultSet);
			}
		};
		// we don't close the Connection nor the ResultSet because it's needed for further reading
		return Iterables.stream(resultSetIterator).collect(Collectors.toSet());
	}
}