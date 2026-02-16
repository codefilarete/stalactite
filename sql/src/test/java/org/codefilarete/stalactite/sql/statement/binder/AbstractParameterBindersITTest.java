package org.codefilarete.stalactite.sql.statement.binder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.assertj.core.data.Offset;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.SQLExecutionException;
import org.codefilarete.stalactite.sql.test.DatabaseIntegrationTest;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.io.IOs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.codefilarete.tool.collection.Iterables.first;

/**
 * Template class for database integration tests of dedicated {@link ParameterBinder}s
 * 
 * @author Guillaume Mary
 */
public abstract class AbstractParameterBindersITTest extends DatabaseIntegrationTest {
	
	protected ParameterBinderRegistry parameterBinderRegistry;
	
	protected JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping;
	
	@BeforeEach
	protected abstract void createParameterBinderRegistry();
	
	@BeforeEach
	protected abstract void createJavaTypeToSqlTypeMapping();
	
	@Test
	protected void longBinder() throws SQLException {
		testParameterBinder(Long.class, Arrays.asSet(null, 42L));
	}
	
	@Test
	protected void longPrimitiveBinder() throws SQLException {
		testParameterBinder(Long.TYPE, Arrays.asSet(42L));
	}
	
	@Test
	protected void longPrimitiveBinder_nullValuePassed_NPEThrown() {
		assertThatCode(() -> testParameterBinder(Long.TYPE, Arrays.asSet(null))).isInstanceOf(NullPointerException.class);
	}
	
	@Test
	protected void integerBinder() throws SQLException {
		testParameterBinder(Integer.class, Arrays.asSet(null, 42));
	}
	
	@Test
	protected void integerPrimitiveBinder() throws SQLException {
		testParameterBinder(Integer.TYPE, Arrays.asSet(42));
	}
	
	@Test
	protected void integerPrimitiveBinder_nullValuePassed_NPEThrown() {
		assertThatCode(() -> testParameterBinder(Integer.TYPE, Arrays.asSet(null))).isInstanceOf(NullPointerException.class);
	}
	
	@Test
	protected void byteBinder() throws SQLException {
		testParameterBinder(Byte.class, Arrays.asSet(null, (byte) 42));
	}
	
	@Test
	protected void bytePrimitiveBinder() throws SQLException {
		testParameterBinder(Byte.TYPE, Arrays.asSet((byte) 42));
	}
	
	@Test
	protected void bytePrimitiveBinder_nullValuePassed_NPEThrown() {
		assertThatCode(() -> testParameterBinder(Byte.TYPE, Arrays.asSet(null))).isInstanceOf(NullPointerException.class);
	}
	
	@Test
	protected void bytesBinder() throws SQLException {
		byte[] inputStream = "Hello world !".getBytes();
		Set<byte[]> valuesToInsert = Arrays.asSet(inputStream, null);
		Set<byte[]> databaseContent = insertAndSelect(byte[].class, valuesToInsert);
		assertThat(convertBytesToString(databaseContent)).isEqualTo(Arrays.asSet(null, "Hello world !"));
	}
	
	static Set<String> convertBytesToString(Set<byte[]> databaseContent) {
		return databaseContent.stream().map(s -> Nullable.nullable(s).map(String::new).get()).collect(Collectors.toSet());
	}
	
	@Test
	protected void doubleBinder() throws SQLException {
		testParameterBinder(Double.class, Arrays.asSet(null, 42.57D));
	}
	
	@Test
	protected void doublePrimitiveBinder() throws SQLException {
		testParameterBinder(Double.TYPE, Arrays.asSet(42.57D));
	}
	
	@Test
	protected void doublePrimitiveBinder_nullValuePassed_NPEThrown() {
		assertThatCode(() -> testParameterBinder(Double.TYPE, Arrays.asSet(null))).isInstanceOf(NullPointerException.class);
	}
	
	@Test
	protected void floatBinder() throws SQLException {
		testParameterBinder(Float.class, Arrays.asSet(null, 42.57F));
	}
	
	@Test
	protected void floatPrimitiveBinder() throws SQLException {
		testParameterBinder(Float.TYPE, Arrays.asSet(42.57F));
	}
	
	@Test
	protected void floatPrimitiveBinder_nullValuePassed_NPEThrown() {
		assertThatCode(() -> testParameterBinder(Float.TYPE, Arrays.asSet(null))).isInstanceOf(NullPointerException.class);
	}
	
	@Test
	protected void bigDecimalBinder() throws SQLException {
		BigDecimal nullInsertion = insertAndSelect(BigDecimal.class, (BigDecimal) null);
		assertThat(nullInsertion).isNull();
		clearSchema();
		BigDecimal real = insertAndSelect(BigDecimal.class, BigDecimal.valueOf(42.66));
		assertThat(real).isCloseTo(BigDecimal.valueOf(42.66), Offset.offset(BigDecimal.valueOf(0.001)));
	}
	
	@Test
	protected void bigIntegerBinder() throws SQLException {
		testParameterBinder(BigInteger.class, Arrays.asSet(null, new BigInteger("42", 10)));
	}
	
	@Test
	protected void booleanBinder() throws SQLException {
		testParameterBinder(Boolean.class, Arrays.asSet(null, true, false));
	}
	
	@Test
	protected void booleanPrimitiveBinder() throws SQLException {
		testParameterBinder(Boolean.TYPE, Arrays.asSet(true, false));
	}
	
	@Test
	protected void booleanPrimitiveBinder_nullValuePassed_NPEThrown() {
		assertThatCode(() -> testParameterBinder(Boolean.TYPE, Arrays.asSet(null))).isInstanceOf(NullPointerException.class);
	}
	
	@Test
	protected void dateSqlBinder() throws SQLException {
		// Using Date(long) constructor leads to mistake since the Date spec (Javadoc) says : "the hours, minutes, seconds, and milliseconds to 
		// zero"  
		// So if the argument is a real millis time (like System.currentTimeMillis), the millis precision is lost by PreparedStatement.setDate()
		// (the data type is not the culprit since timestamp makes the same result), hence the comparison with the select result fails
		// Therefore we use a non-precised millis thanks to LocalDate.now()
		java.sql.Date date = java.sql.Date.valueOf(LocalDate.now());
		testParameterBinder(java.sql.Date.class, Arrays.asSet(null, date));
	}
	
	@Test
	protected void dateBinder() throws SQLException {
		// Using Date(long) constructor leads to mistake since the Date spec (Javadoc) says : "the hours, minutes, seconds, and milliseconds to 
		// zero"  
		// So if the argument is a real millis time (like System.currentTimeMillis), the millis precision is lost by PreparedStatement.setDate()
		// (the data type is not the culprit since timestamp makes the same result), hence the comparison with the select result fails
		// Therefore we use a non-precised millis thanks to LocalDate.now()
		java.util.Date date = Date.from(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant());
		testParameterBinder(java.util.Date.class, Arrays.asSet(null, date));
	}
	
	@Test
	protected void localDateBinder() throws SQLException {
		testParameterBinder(LocalDate.class, Arrays.asSet(null, LocalDate.now()));
	}
	
	@Test
	protected void localDateTimeBinder() throws SQLException {
		// Since Java 9 LocalDateTime.now() changed its precision : when available by OS it takes nanosecond precision,
		// (https://bugs.openjdk.java.net/browse/JDK-8068730)
		// this implies a comparison failure because many databases don't store nanosecond by default (with SQL TIMESTAMP type, which is the default
		// in DefaultTypeMapping), therefore the LocalDateTime read by binder doesn't contain nanoseconds, so when it is compared to original value
		// (which contains nanos) it fails. To overcome this problem we consider not using LocalDateTime.now(), and taking the loss of precision
		// in the test
		LocalDateTime initialTime = LocalDateTime.of(2021, Month.JULY, 12, 4, 23, 35, 123456789);
		LocalDateTime comparisonTime = LocalDateTime.of(2021, Month.JULY, 12, 4, 23, 35, 123456000);
		Set<LocalDateTime> databaseContent = insertAndSelect(LocalDateTime.class, Arrays.asSet(null, initialTime));
		assertThat(databaseContent).isEqualTo(Arrays.asSet(null, comparisonTime));
	}
	
	@Test
	protected void localTimeBinder() throws SQLException {
		// Since Java 9 LocalDateTime.now() changed its precision : when available by OS it takes nanosecond precision,
		// (https://bugs.openjdk.java.net/browse/JDK-8068730)
		// this implies a comparison failure because many databases don't store nanosecond by default (with SQL TIMESTAMP type, which is the default
		// in DefaultTypeMapping), therefore the LocalDateTime read by binder doesn't contain nanoseconds, so when it is compared to original value
		// (which contains nanos) it fails. To overcome this problem we consider not using LocalDateTime.now(), and taking the loss of precision
		// in the test
		LocalTime initialTime = LocalTime.of(4, 23, 35, 123456789);
		LocalTime comparisonTime = LocalTime.of(4, 23, 35, 123456000);
		Set<LocalTime> databaseContent = insertAndSelect(LocalTime.class, Arrays.asSet(null, initialTime));
		assertThat(databaseContent).isEqualTo(Arrays.asSet(null, comparisonTime));
	}
	
	@Test
	protected void timestampBinder() throws SQLException {
		testParameterBinder(Timestamp.class, Arrays.asSet(null, new Timestamp(System.currentTimeMillis())));
	}
	
	@Test
	protected void stringBinder() throws SQLException {
		testParameterBinder(parameterBinderRegistry.getBinder(String.class), "varchar(255)", Arrays.asSet(null, "Hello world !"));
	}
	
	@Test
	protected void binaryStreamBinder() throws SQLException {
		InputStream inputStream = new ByteArrayInputStream("Hello world !".getBytes());
		LinkedHashSet<InputStream> valuesToInsert = Arrays.asSet(inputStream, null);
		Set<InputStream> databaseContent = insertAndSelect(InputStream.class, valuesToInsert);
		assertThat(convertInputStreamToString(databaseContent)).isEqualTo(Arrays.asSet(null, "Hello world !"));
	}
	
	@Test
	protected void blobBinder() throws SQLException {
		Blob blob = new InMemoryBlobSupport("Hello world !".getBytes());
		Set<Blob> valuesToInsert = Arrays.asSet(blob, null);
		Set<Blob> databaseContent = insertAndSelect(Blob.class, valuesToInsert);
		assertThat(convertBlobToString(databaseContent)).isEqualTo(Arrays.asSet(null, "Hello world !"));
	}
	
	@Test
	protected void uuidBinder() throws SQLException {
		testParameterBinder(UUID.class, Arrays.asSet(null, UUID.randomUUID()));
	}
	
	@Test
	protected void pathBinder() throws SQLException {
		testParameterBinder(Path.class, Arrays.asSet(null, Paths.get("/path/to/my/file")));
	}
	
	@Test
	protected void fileBinder() throws SQLException {
		testParameterBinder(File.class, Arrays.asSet(null, new File("/path/to/my/file")));
	}
	
	protected <T> void testParameterBinder(Class<T> typeToTest, Set<T> valuesToInsert) throws SQLException {
		Set<T> databaseContent = insertAndSelect(typeToTest, valuesToInsert);
		assertThat(databaseContent).isEqualTo(valuesToInsert);
	}
	
	protected <T> void testParameterBinder(ParameterBinder<T> testInstance, String sqlColumnType, Set<T> valuesToInsert) throws SQLException {
		Set<T> databaseContent = insertAndSelect(testInstance, sqlColumnType, valuesToInsert, connectionProvider.giveConnection());
		assertThat(databaseContent).isEqualTo(valuesToInsert);
	}
	
	static Set<String> convertInputStreamToString(Set<InputStream> databaseContent) {
		return databaseContent.stream().map(s -> Nullable.nullable(s).map(inputStream -> {
			try (InputStream closeable = inputStream) {
				return new String(IOs.toByteArray(closeable));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}).get()).collect(Collectors.toSet());
	}
	
	protected static Set<String> convertBlobToString(Set<Blob> databaseContent) {
		return databaseContent.stream().map(b -> {
			try {
				return Nullable.nullable(b).mapThrower(blob -> new String(blob.getBytes(1, (int) blob.length()))).get();
			} catch (SQLException e) {
				throw new SQLExecutionException(e);
			}
		}).collect(Collectors.toSet());
	}
	
	protected <T> T insertAndSelect(Class<T> typeToTest, T valuesToInsert) throws SQLException {
		ParameterBinder<T> testInstance = parameterBinderRegistry.getBinder(typeToTest);
		String sqlColumnType = javaTypeToSqlTypeMapping.getTypeName(typeToTest);
		return first(insertAndSelect(testInstance, sqlColumnType, Arrays.asSet(valuesToInsert), connectionProvider.giveConnection()));
	}
	
	protected <T> Set<T> insertAndSelect(Class<T> typeToTest, Set<T> valuesToInsert) throws SQLException {
		ParameterBinder<T> testInstance = parameterBinderRegistry.getBinder(typeToTest);
		String sqlColumnType = javaTypeToSqlTypeMapping.getTypeName(typeToTest);
		return insertAndSelect(testInstance, sqlColumnType, valuesToInsert, connectionProvider.giveConnection());
	}
	
	protected void clearSchema() {
		try {
			connectionProvider.giveConnection().prepareStatement("drop table Toto").execute();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	protected <T> Set<T> insertAndSelect(ParameterBinder<T> testInstance, String sqlColumnType, Set<T> valuesToInsert, Connection connection) throws SQLException {
		connection.prepareStatement("create table Toto(a " + sqlColumnType + ")").execute();
		// Test of ParameterBinder#set
		PreparedStatement statement = connection.prepareStatement("insert into Toto(a) values (?)");
		valuesToInsert.forEach(v -> {
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
