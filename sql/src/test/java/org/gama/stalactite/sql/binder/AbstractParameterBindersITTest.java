package org.gama.stalactite.sql.binder;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.gama.lang.Nullable;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.io.IOs;
import org.gama.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.sql.dml.SQLExecutionException;
import org.gama.stalactite.sql.result.ResultSetIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Template class for database integration tests of dedicated {@link ParameterBinder}s 
 * 
 * @author Guillaume Mary
 */
public abstract class AbstractParameterBindersITTest {

    protected DataSource dataSource;
    
    protected ParameterBinderRegistry parameterBinderRegistry;
    
    protected JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping;

    @BeforeEach
    abstract void createDataSource();

    @BeforeEach
    abstract void createParameterBinderRegistry();

    @BeforeEach
    abstract void createJavaTypeToSqlTypeMapping();

    @Test
    void longBinder() throws SQLException {
        testParameterBinder(Long.class, Arrays.asSet(null, 42L));
    }

    @Test
    void longPrimitiveBinder() throws SQLException {
        testParameterBinder(Long.TYPE, Arrays.asSet(42L));
    }

    @Test
    void longPrimitiveBinder_nullValuePassed_NPEThrown() throws SQLException {
        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> testParameterBinder(Long.TYPE, Arrays.asSet(null)));
    }

    @Test
    void integerBinder() throws SQLException {
        testParameterBinder(Integer.class, Arrays.asSet(null, 42));
    }

    @Test
    void integerPrimitiveBinder() throws SQLException {
        testParameterBinder(Integer.TYPE, Arrays.asSet(42));
    }

    @Test
    void integerPrimitiveBinder_nullValuePassed_NPEThrown() throws SQLException {
        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> testParameterBinder(Integer.TYPE, Arrays.asSet(null)));
    }

    @Test
    void byteBinder() throws SQLException {
        testParameterBinder(Byte.class, Arrays.asSet(null, (byte) 42));
    }

    @Test
    void bytePrimitiveBinder() throws SQLException {
        testParameterBinder(Byte.TYPE, Arrays.asSet((byte) 42));
    }

    @Test
    void bytePrimitiveBinder_nullValuePassed_NPEThrown() throws SQLException {
        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> testParameterBinder(Byte.TYPE, Arrays.asSet(null)));
    }

    @Test
    void bytesBinder() throws SQLException {
        byte[] inputStream = "Hello world !".getBytes();
        Set<byte[]> valuesToInsert = Arrays.asSet(inputStream, null);
        Set<byte[]> databaseContent = insertAndSelect(byte[].class, valuesToInsert);
        assertThat(convertBytesToString(databaseContent)).isEqualTo(Arrays.asSet(null, "Hello world !"));
    }

    static Set<String> convertBytesToString(Set<byte[]> databaseContent) {
        return databaseContent.stream().map(s -> Nullable.nullable(s).map(String::new).get()).collect(Collectors.toSet());
    }

    @Test
    void doubleBinder() throws SQLException {
        testParameterBinder(Double.class, Arrays.asSet(null, 42.57D));
    }

    @Test
    void doublePrimitiveBinder() throws SQLException {
        testParameterBinder(Double.TYPE, Arrays.asSet(42.57D));
    }

    @Test
    void doublePrimitiveBinder_nullValuePassed_NPEThrown() throws SQLException {
        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> testParameterBinder(Double.TYPE, Arrays.asSet(null)));
    }

    @Test
    void floatBinder() throws SQLException {
        testParameterBinder(Float.class, Arrays.asSet(null, 42.57F));
    }

    @Test
    void floatPrimitiveBinder() throws SQLException {
        testParameterBinder(Float.TYPE, Arrays.asSet(42.57F));
    }

    @Test
    void floatPrimitiveBinder_nullValuePassed_NPEThrown() throws SQLException {
        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> testParameterBinder(Float.TYPE, Arrays.asSet(null)));
    }

    @Test
    void bigDecimalBinder() throws SQLException {
        testParameterBinder(BigDecimal.class, Arrays.asSet(null, new BigDecimal("42.6600", new MathContext(6))));
    }

    @Test
    void booleanBinder() throws SQLException {
        testParameterBinder(Boolean.class, Arrays.asSet(null, true, false));
    }

    @Test
    void booleanPrimitiveBinder() throws SQLException {
        testParameterBinder(Boolean.TYPE, Arrays.asSet(true, false));
    }

    @Test
    void booleanPrimitiveBinder_nullValuePassed_NPEThrown() throws SQLException {
        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> testParameterBinder(Boolean.TYPE, Arrays.asSet(null)));
    }

    @Test
    void dateSqlBinder() throws SQLException {
        // Using Date(long) constructor leads to mistake since the Date spec (Javadoc) says : "the hours, minutes, seconds, and milliseconds to zero"  
        // So if the argument is a real millis time (like System.currentTimeMillis), the millis precision is lost by PreparedStatement.setDate()
        // (the data type is not the culprit since timestamp makes the same result), hence the comparison with the select result fails
        // Therefore we use a non-precised millis thanks to LocalDate.now()
        java.sql.Date date = java.sql.Date.valueOf(LocalDate.now());
        testParameterBinder(java.sql.Date.class, Arrays.asSet(null, date));
    }

    @Test
    void dateBinder() throws SQLException {
        // Using Date(long) constructor leads to mistake since the Date spec (Javadoc) says : "the hours, minutes, seconds, and milliseconds to zero"  
        // So if the argument is a real millis time (like System.currentTimeMillis), the millis precision is lost by PreparedStatement.setDate()
        // (the data type is not the culprit since timestamp makes the same result), hence the comparison with the select result fails
        // Therefore we use a non-precised millis thanks to LocalDate.now()
        java.util.Date date = Date.from(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant());
        testParameterBinder(java.util.Date.class, Arrays.asSet(null, date));
    }

    @Test
    void localDateBinder() throws SQLException {
        testParameterBinder(LocalDate.class, Arrays.asSet(null, LocalDate.now()));
    }

    @Test
    void localDateTimeBinder() throws SQLException {
        testParameterBinder(LocalDateTime.class, Arrays.asSet(null, LocalDateTime.now()));
    }

    @Test
    void timestampBinder() throws SQLException {
        testParameterBinder(Timestamp.class, Arrays.asSet(null, new Timestamp(System.currentTimeMillis())));
    }

    @Test
    void stringBinder() throws SQLException {
        testParameterBinder(parameterBinderRegistry.getBinder(String.class), "varchar(255)", Arrays.asSet(null, "Hello world !"));
    }

    @Test
    void binaryStreamBinder() throws SQLException {
        InputStream inputStream = new ByteArrayInputStream("Hello world !".getBytes());
        LinkedHashSet<InputStream> valuesToInsert = Arrays.asSet(inputStream, null);
        Set<InputStream> databaseContent = insertAndSelect(InputStream.class, valuesToInsert);
        assertThat(convertInputStreamToString(databaseContent)).isEqualTo(Arrays.asSet(null, "Hello world !"));
    }


    @Test
    void blobBinder() throws SQLException {
        Blob blob = new InMemoryBlobSupport("Hello world !".getBytes());
        Set<Blob> valuesToInsert = Arrays.asSet(blob, null);
        Set<Blob> databaseContent = insertAndSelect(Blob.class, valuesToInsert);
        assertThat(convertBlobToString(databaseContent)).isEqualTo(Arrays.asSet(null, "Hello world !"));
    }

    private <T> void testParameterBinder(Class<T> typeToTest, Set<T> valuesToInsert) throws SQLException {
        Set<T> databaseContent = insertAndSelect(typeToTest, valuesToInsert);
        assertThat(databaseContent).isEqualTo(valuesToInsert);
    }

    protected <T> void testParameterBinder(ParameterBinder<T> testInstance, String sqlColumnType, Set<T> valuesToInsert) throws SQLException {
        Set<T> databaseContent = insertAndSelect(testInstance, sqlColumnType, valuesToInsert, dataSource.getConnection());
        assertThat(databaseContent).isEqualTo(valuesToInsert);
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

    static Set<String> convertBlobToString(Set<Blob> databaseContent) {
        return databaseContent.stream().map(b -> {
            try {
                return Nullable.nullable(b).mapThrower(blob -> new String(blob.getBytes(1, (int) blob.length()))).get();
            } catch (SQLException e) {
                throw new SQLExecutionException(e);
            }
        }).collect(Collectors.toSet());
    }

    protected <T> Set<T> insertAndSelect(Class<T> typeToTest, Set<T> valuesToInsert) throws SQLException {
        ParameterBinder<T> testInstance = parameterBinderRegistry.getBinder(typeToTest);
        String sqlColumnType = javaTypeToSqlTypeMapping.getTypeName(typeToTest);
        return insertAndSelect(testInstance, sqlColumnType, valuesToInsert, dataSource.getConnection());
    }

    protected <T> Set<T> insertAndSelect(ParameterBinder<T> testInstance, String sqlColumnType, Set<T> valuesToInsert, Connection connection) throws SQLException {
        connection.prepareStatement("create table Toto(a "+sqlColumnType+")").execute();
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
