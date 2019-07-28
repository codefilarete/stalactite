package org.gama.stalactite.sql.binder;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.gama.lang.Nullable;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.io.IOs;
import org.gama.stalactite.sql.dml.SQLExecutionException;
import org.gama.stalactite.sql.result.ResultSetIterator;
import org.gama.stalactite.sql.test.DerbyInMemoryDataSource;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.sql.test.MariaDBEmbeddableDataSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Guillaume Mary
 */
public class DefaultParameterBindersTest {
	
	public static Object[][] dataSources() {
		return new Object[][] {
				{ new HSQLDBInMemoryDataSource() },
				{ new DerbyInMemoryDataSource() },
				{ new MariaDBEmbeddableDataSource(3406) },
		};
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testLongBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.LONG_BINDER, dataSource, "int", Arrays.asSet(null, 42L));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testLongPrimitiveBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER, dataSource, "int not null", Arrays.asSet(42L));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testLongPrimitiveBinder_nullValuePassed_NPEThrown(DataSource dataSource) throws SQLException {
		assertThrows(NullPointerException.class, () -> testParameterBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER, dataSource,
				"int not null", Arrays.asSet(null)));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testIntegerBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.INTEGER_BINDER, dataSource, "int", Arrays.asSet(null, 42));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testIntegerPrimitiveBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER, dataSource, "int not null", Arrays.asSet(42));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testIntegerPrimitiveBinder_nullValuePassed_NPEThrown(DataSource dataSource) throws SQLException {
		assertThrows(NullPointerException.class, () -> testParameterBinder(DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER, dataSource,
				"int not null", Arrays.asSet(null)));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testByteBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.BYTE_BINDER, dataSource, "int", Arrays.asSet(null, (byte) 42));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testBytePrimitiveBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.BYTE_PRIMITIVE_BINDER, dataSource, "int not null", Arrays.asSet((byte) 42));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testBytePrimitiveBinder_nullValuePassed_NPEThrown(DataSource dataSource) throws SQLException {
		assertThrows(NullPointerException.class, () -> testParameterBinder(DefaultParameterBinders.BYTE_PRIMITIVE_BINDER, dataSource,
				"int not null", Arrays.asSet(null)));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testBytesBinder(DataSource dataSource) throws SQLException {
		byte[] inputStream = "Hello world !".getBytes();
		Set<byte[]> valuesToInsert = Arrays.asSet(inputStream, null);
		ParameterBinder<byte[]> binarystreamBinder = DefaultParameterBinders.BYTES_BINDER;
		if (dataSource instanceof DerbyInMemoryDataSource) {
			binarystreamBinder = DerbyParameterBinders.BYTES_BINDER;
		}
		Set<byte[]> databaseContent = insertAndSelect(dataSource, binarystreamBinder, "blob", valuesToInsert);
		assertEquals(Arrays.asSet(null, "Hello world !"), convertBytesToString(databaseContent));
	}
	
	static Set<String> convertBytesToString(Set<byte[]> databaseContent) {
		return databaseContent.stream().map(s -> Nullable.nullable(s).map(String::new).get()).collect(Collectors.toSet());
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testDoubleBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.DOUBLE_BINDER, dataSource, "double", Arrays.asSet(null, 42.57D));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testDoublePrimitiveBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.DOUBLE_PRIMITIVE_BINDER, dataSource, "double not null", Arrays.asSet(42.57D));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testDoublePrimitiveBinder_nullValuePassed_NPEThrown(DataSource dataSource) throws SQLException {
		assertThrows(NullPointerException.class, () -> testParameterBinder(DefaultParameterBinders.DOUBLE_PRIMITIVE_BINDER, dataSource,
				"double not null", Arrays.asSet(null)));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testFloatBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.FLOAT_BINDER, dataSource, "double", Arrays.asSet(null, 42.57F));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testFloatPrimitiveBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.FLOAT_PRIMITIVE_BINDER, dataSource, "double not null", Arrays.asSet(42.57F));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testFloatPrimitiveBinder_nullValuePassed_NPEThrown(DataSource dataSource) throws SQLException {
		assertThrows(NullPointerException.class, () -> testParameterBinder(DefaultParameterBinders.FLOAT_PRIMITIVE_BINDER, dataSource,
				"double not null", Arrays.asSet(null)));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testBigDecimalBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.BIGDECIMAL_BINDER, dataSource, "DECIMAL(10, 4)", Arrays.asSet(null, new BigDecimal(42.66, new MathContext(6))));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testBooleanBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.BOOLEAN_BINDER, dataSource, "boolean", Arrays.asSet(null, true, false));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testBooleanPrimitiveBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.BOOLEAN_PRIMITIVE_BINDER, dataSource, "boolean not null", Arrays.asSet(true, false));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testBooleanPrimitiveBinder_nullValuePassed_NPEThrown(DataSource dataSource) throws SQLException {
		assertThrows(NullPointerException.class, () -> testParameterBinder(DefaultParameterBinders.BOOLEAN_PRIMITIVE_BINDER, dataSource,
				"boolean not null", Arrays.asSet(null)));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testDateSqlBinder(DataSource dataSource) throws SQLException {
		// Using Date(long) constructor leads to mistake since the Date spec (Javadoc) says : "the hours, minutes, seconds, and milliseconds to zero"  
		// So if the argument is a real millis time (like System.currentTimeMillis), the millis precision is lost by PreparedStatement.setDate()
		// (the data type is not the culprit since timestamp makes the same result), hence the comparison with the select result fails
		// Therefore we use a non-precised millis thanks to LocalDate.now()
		java.sql.Date date = java.sql.Date.valueOf(LocalDate.now());
		testParameterBinder(DefaultParameterBinders.DATE_SQL_BINDER, dataSource, "date", Arrays.asSet(null, date));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testDateBinder(DataSource dataSource) throws SQLException {
		// Using Date(long) constructor leads to mistake since the Date spec (Javadoc) says : "the hours, minutes, seconds, and milliseconds to zero"  
		// So if the argument is a real millis time (like System.currentTimeMillis), the millis precision is lost by PreparedStatement.setDate()
		// (the data type is not the culprit since timestamp makes the same result), hence the comparison with the select result fails
		// Therefore we use a non-precised millis thanks to LocalDate.now()
		Date date = Date.from(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant());
		testParameterBinder(DefaultParameterBinders.DATE_BINDER, dataSource, "date", Arrays.asSet(null, date));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testLocalDateBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.LOCALDATE_BINDER, dataSource, "date", Arrays.asSet(null, LocalDate.now()));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testLocalDateTimeBinder(DataSource dataSource) throws SQLException {
		String sqlColumnType = "timestamp";
		if (dataSource instanceof MariaDBEmbeddableDataSource) {
			sqlColumnType = "timestamp(3) null"; // 3 for nano precision, else nanos are lost during storage, hence comparison fails
		}
		testParameterBinder(DefaultParameterBinders.LOCALDATETIME_BINDER, dataSource, sqlColumnType, Arrays.asSet(null, LocalDateTime.now()));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testTimestampBinder(DataSource dataSource) throws SQLException {
		String sqlColumnType = "timestamp";
		if (dataSource instanceof MariaDBEmbeddableDataSource) {
			sqlColumnType = "timestamp(3) null"; // 3 for nano precision, else nanos are lost during storage, hence comparison fails
		}
		testParameterBinder(DefaultParameterBinders.TIMESTAMP_BINDER, dataSource, sqlColumnType, Arrays.asSet(null, new Timestamp(System.currentTimeMillis())));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testStringBinder(DataSource dataSource) throws SQLException {
		testParameterBinder(DefaultParameterBinders.STRING_BINDER, dataSource, "varchar(255)", Arrays.asSet(null, "Hello world !"));
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
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
		assertEquals(Arrays.asSet(null, "Hello world !"), convertInputStreamToString(databaseContent));
	}
	
	static Set<String> convertInputStreamToString(Set<InputStream> databaseContent) {
		return databaseContent.stream().map(s -> Nullable.nullable(s).map(inputStream -> {
			try(InputStream closeable = inputStream) {
				return new String(IOs.toByteArray(closeable));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}).get()).collect(Collectors.toSet());
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testBlobBinder(DataSource dataSource) throws SQLException {
		Blob blob = new InMemoryBlobSupport("Hello world !".getBytes());
		Set<Blob> valuesToInsert = Arrays.asSet(blob, null);
		ParameterBinder<Blob> binarystreamBinder = DefaultParameterBinders.BLOB_BINDER;
		if (dataSource instanceof DerbyInMemoryDataSource) {
			binarystreamBinder = DerbyParameterBinders.BLOB_BINDER;
		}
		Set<Blob> databaseContent = insertAndSelect(dataSource, binarystreamBinder, "blob", valuesToInsert);
		assertEquals(Arrays.asSet(null, "Hello world !"), convertBlobToString(databaseContent));
	}
	
	static Set<String> convertBlobToString(Set<Blob> databaseContent) {
		return databaseContent.stream().map(b -> {
			try {
				return Nullable.nullable(b).mapThrower(blob -> new String(blob.getBytes(1, (int) blob.length()))).get();
			} catch (SQLException e) {
				throw new SQLExecutionException(e);
			}
		}).collect(Collectors.toSet());
	}
	
	@ParameterizedTest
	@MethodSource("dataSources")
	public void testBlobBinder_inputStream(DataSource dataSource) throws SQLException {
		InputStream inputStream = new ByteArrayInputStream("Hello world !".getBytes());
		Set<InputStream> valuesToInsert = Arrays.asSet(inputStream, null);
		ParameterBinder<InputStream> binarystreamBinder = DefaultParameterBinders.BLOB_INPUTSTREAM_BINDER;
		if (dataSource instanceof DerbyInMemoryDataSource) {
			binarystreamBinder = DerbyParameterBinders.BLOB_INPUTSTREAM_BINDER;
		}
		Set<InputStream> databaseContent = insertAndSelect(dataSource, binarystreamBinder, "blob", valuesToInsert);
		assertEquals(Arrays.asSet(null, "Hello world !"), convertInputStreamToString(databaseContent));
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
				testInstance.set(statement, 1, v);
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
				return testInstance.get(resultSet, "a");
			}
		};
		// we don't close the Connection nor the ResultSet because it's needed for further reading
		return Iterables.stream(resultSetIterator).collect(Collectors.toSet());
	}
}