package org.gama.sql.binder;

import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

import org.gama.lang.Nullable;
import org.gama.sql.dml.SQLExecutionException;

import static org.gama.sql.binder.DefaultPreparedStatementWriters.BIGDECIMAL_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.BINARYSTREAM_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.BLOB_INPUTSTREAM_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.BLOB_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.BOOLEAN_PRIMITIVE_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.BYTES_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.BYTE_PRIMITIVE_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.DATE_SQL_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.DOUBLE_PRIMITIVE_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.FLOAT_PRIMITIVE_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.INTEGER_PRIMITIVE_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.LONG_PRIMITIVE_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.STRING_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.TIMESTAMP_WRITER;
import static org.gama.sql.binder.DefaultPreparedStatementWriters.URL_WRITER;
import static org.gama.sql.binder.DefaultResultSetReaders.BIGDECIMAL_READER;
import static org.gama.sql.binder.DefaultResultSetReaders.BINARYSTREAM_READER;
import static org.gama.sql.binder.DefaultResultSetReaders.BLOB_READER;
import static org.gama.sql.binder.DefaultResultSetReaders.BOOLEAN_PRIMITIVE_READER;
import static org.gama.sql.binder.DefaultResultSetReaders.BYTES_READER;
import static org.gama.sql.binder.DefaultResultSetReaders.BYTE_PRIMITIVE_READER;
import static org.gama.sql.binder.DefaultResultSetReaders.DATE_SQL_READER;
import static org.gama.sql.binder.DefaultResultSetReaders.DOUBLE_PRIMITIVE_READER;
import static org.gama.sql.binder.DefaultResultSetReaders.FLOAT_PRIMITIVE_READER;
import static org.gama.sql.binder.DefaultResultSetReaders.INTEGER_PRIMITIVE_READER;
import static org.gama.sql.binder.DefaultResultSetReaders.LONG_PRIMITIVE_READER;
import static org.gama.sql.binder.DefaultResultSetReaders.STRING_READER;
import static org.gama.sql.binder.DefaultResultSetReaders.TIMESTAMP_READER;
import static org.gama.sql.binder.DefaultResultSetReaders.URL_READER;

/**
 * Default {@link ParameterBinder}s mapped to methods of {@link ResultSet} and {@link PreparedStatement} 
 * 
 * @author Guillaume Mary
 */
public interface DefaultParameterBinders {
	
	/* Implementation note: although primitive binders are parameterized with generics they are not wrapped by a NullAwareParameterBinder
	 * so they'll throw an exception or convert Object-typed value passed if null is writen or read.
	 */
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getLong(String)} and {@link PreparedStatement#setLong(int, long)}.
	 */
	ParameterBinder<Long> LONG_PRIMITIVE_BINDER = new LambdaParameterBinder<>(LONG_PRIMITIVE_READER, LONG_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getLong(String)} and {@link PreparedStatement#setLong(int, long)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	ParameterBinder<Long> LONG_BINDER = new NullAwareParameterBinder<>(LONG_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getInt(String)} and {@link PreparedStatement#setInt(int, int)}.
	 */
	ParameterBinder<Integer> INTEGER_PRIMITIVE_BINDER = new LambdaParameterBinder<>(INTEGER_PRIMITIVE_READER, INTEGER_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getInt(String)} and {@link PreparedStatement#setInt(int, int)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	ParameterBinder<Integer> INTEGER_BINDER = new NullAwareParameterBinder<>(INTEGER_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getByte(String)} and {@link PreparedStatement#setByte(int, byte)}.
	 */
	ParameterBinder<Byte> BYTE_PRIMITIVE_BINDER = new LambdaParameterBinder<>(BYTE_PRIMITIVE_READER, BYTE_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getByte(String)} and {@link PreparedStatement#setByte(int, byte)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	ParameterBinder<Byte> BYTE_BINDER = new NullAwareParameterBinder<>(BYTE_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBytes(String)} and {@link PreparedStatement#setBytes(int, byte[])}.
	 */
	ParameterBinder<byte[]> BYTES_BINDER = new NullAwareParameterBinder<>(new LambdaParameterBinder<>(BYTES_READER, BYTES_WRITER));
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getDouble(String)} and {@link PreparedStatement#setDouble(int, double)}.
	 */
	ParameterBinder<Double> DOUBLE_PRIMITIVE_BINDER = new LambdaParameterBinder<>(DOUBLE_PRIMITIVE_READER, DOUBLE_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getDouble(String)} and {@link PreparedStatement#setDouble(int, double)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	ParameterBinder<Double> DOUBLE_BINDER = new NullAwareParameterBinder<>(DOUBLE_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getFloat(String)} and {@link PreparedStatement#setFloat(int, float)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	ParameterBinder<Float> FLOAT_PRIMITIVE_BINDER = new LambdaParameterBinder<>(FLOAT_PRIMITIVE_READER, FLOAT_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getFloat(String)} and {@link PreparedStatement#setFloat(int, float)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	ParameterBinder<Float> FLOAT_BINDER = new NullAwareParameterBinder<>(FLOAT_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBigDecimal(String)} and {@link PreparedStatement#setBigDecimal(int, BigDecimal)}.
	 */
	ParameterBinder<BigDecimal> BIGDECIMAL_BINDER = new LambdaParameterBinder<>(BIGDECIMAL_READER, BIGDECIMAL_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBoolean(String)} and {@link PreparedStatement#setBoolean(int, boolean)}.
	 */
	ParameterBinder<Boolean> BOOLEAN_PRIMITIVE_BINDER = new LambdaParameterBinder<>(BOOLEAN_PRIMITIVE_READER, BOOLEAN_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBoolean(String)} and {@link PreparedStatement#setBoolean(int, boolean)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	ParameterBinder<Boolean> BOOLEAN_BINDER = new NullAwareParameterBinder<>(BOOLEAN_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getDate(String)} and {@link PreparedStatement#setDate(int, Date)}.
	 * For common usage, prefer {@link DateBinder} because it uses {@link java.util.Date}
	 * @see DateBinder
	 */
	ParameterBinder<Date> DATE_SQL_BINDER = new LambdaParameterBinder<>(DATE_SQL_READER, DATE_SQL_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getTimestamp(String)} and {@link PreparedStatement#setTimestamp(int, Timestamp)}.
	 */
	ParameterBinder<Timestamp> TIMESTAMP_BINDER = new LambdaParameterBinder<>(TIMESTAMP_READER, TIMESTAMP_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getString(String)} and {@link PreparedStatement#setString(int, String)}.
	 */
	ParameterBinder<String> STRING_BINDER = new LambdaParameterBinder<>(STRING_READER, STRING_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBinaryStream(String)} and {@link PreparedStatement#setBinaryStream(int, InputStream)}.
	 * @see DerbyParameterBinders#BINARYSTREAM_BINDER
	 * @see HSQLDBParameterBinders#BINARYSTREAM_BINDER
	 * @see #BLOB_BINDER
	 * @see #BLOB_INPUTSTREAM_BINDER
	 */
	ParameterBinder<InputStream> BINARYSTREAM_BINDER = new LambdaParameterBinder<>(BINARYSTREAM_READER, BINARYSTREAM_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getURL(String)} and {@link PreparedStatement#setURL(int, URL)}.
	 */
	ParameterBinder<URL> URL_BINDER = new LambdaParameterBinder<>(URL_READER, URL_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBlob(String)} and {@link PreparedStatement#setBlob(int, Blob)}.
	 * 
	 * @see #BLOB_INPUTSTREAM_BINDER
	 * @see #BINARYSTREAM_BINDER
	 */
	ParameterBinder<Blob> BLOB_BINDER = new LambdaParameterBinder<>(BLOB_READER, BLOB_WRITER);
	
	/**
	 * Binder for {@link InputStream}. Differs from {@link #BLOB_BINDER} because it uses {@link PreparedStatement#setBlob(int, InputStream)}
	 * for writing and {@link ResultSet#getBlob(String)} plus {@link Blob#getBinaryStream}.
	 * Main difference stays in JDBC column type and may causes performance issue : Blob is expected with {@link PreparedStatement#setBlob(int, Blob)}
	 * whereas LONGVARBINARY is expected with {@link PreparedStatement#setBlob(int, InputStream)} or {@link PreparedStatement#setBinaryStream(int, InputStream)}
	 * 
	 * @see #BLOB_BINDER
	 * @see #BINARYSTREAM_BINDER
	 */
	ParameterBinder<InputStream> BLOB_INPUTSTREAM_BINDER = new LambdaParameterBinder<>(BLOB_READER.thenApply(stream -> {
		try {
			return Nullable.nullable(stream).mapThrower(Blob::getBinaryStream).get();
		} catch (SQLException e) {
			throw new SQLExecutionException(e);
		}
	}), BLOB_INPUTSTREAM_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link java.util.Date}
	 */
	ParameterBinder<java.util.Date> DATE_BINDER = new NullAwareParameterBinder<>(new DateBinder());
	
	/**
	 * {@link ParameterBinder} for {@link java.time.LocalDate}
	 */
	ParameterBinder<LocalDate> LOCALDATE_BINDER = new NullAwareParameterBinder<>(new LocalDateBinder());
	
	/**
	 * {@link ParameterBinder} for {@link java.time.LocalDateTime}
	 */
	ParameterBinder<LocalDateTime> LOCALDATETIME_BINDER = new NullAwareParameterBinder<>(new LocalDateTimeBinder());
	
	/**
	 * {@link ParameterBinder} for {@link java.time.ZoneId}.
	 * It may have little purpose alone, but has its interest with {@link java.time.ZonedDateTime}.
	 */
	ParameterBinder<ZoneId> ZONEID_BINDER = new NullAwareParameterBinder<>(new ZoneIdBinder());
	
	/**
	 * {@link ParameterBinder} for {@link UUID}.
	 */
	ParameterBinder<UUID> UUID_PARAMETER_BINDER = new NullAwareParameterBinder<>(new UUIDParameterBinder());
	
}
