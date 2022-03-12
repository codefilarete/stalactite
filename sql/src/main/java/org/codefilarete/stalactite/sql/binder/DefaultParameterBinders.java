package org.codefilarete.stalactite.sql.binder;

import java.io.File;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Blob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.UUID;

import static org.codefilarete.stalactite.sql.binder.DefaultPreparedStatementWriters.BIGDECIMAL_WRITER;
import static org.codefilarete.stalactite.sql.binder.DefaultPreparedStatementWriters.BINARYSTREAM_WRITER;
import static org.codefilarete.stalactite.sql.binder.DefaultPreparedStatementWriters.BLOB_WRITER;
import static org.codefilarete.stalactite.sql.binder.DefaultPreparedStatementWriters.BOOLEAN_PRIMITIVE_WRITER;
import static org.codefilarete.stalactite.sql.binder.DefaultPreparedStatementWriters.BYTES_WRITER;
import static org.codefilarete.stalactite.sql.binder.DefaultPreparedStatementWriters.BYTE_PRIMITIVE_WRITER;
import static org.codefilarete.stalactite.sql.binder.DefaultPreparedStatementWriters.DATE_SQL_WRITER;
import static org.codefilarete.stalactite.sql.binder.DefaultPreparedStatementWriters.DOUBLE_PRIMITIVE_WRITER;
import static org.codefilarete.stalactite.sql.binder.DefaultPreparedStatementWriters.FLOAT_PRIMITIVE_WRITER;
import static org.codefilarete.stalactite.sql.binder.DefaultPreparedStatementWriters.INTEGER_PRIMITIVE_WRITER;
import static org.codefilarete.stalactite.sql.binder.DefaultPreparedStatementWriters.LONG_PRIMITIVE_WRITER;
import static org.codefilarete.stalactite.sql.binder.DefaultPreparedStatementWriters.STRING_WRITER;
import static org.codefilarete.stalactite.sql.binder.DefaultPreparedStatementWriters.TIMESTAMP_WRITER;
import static org.codefilarete.stalactite.sql.binder.DefaultResultSetReaders.BIGDECIMAL_READER;
import static org.codefilarete.stalactite.sql.binder.DefaultResultSetReaders.BINARYSTREAM_READER;
import static org.codefilarete.stalactite.sql.binder.DefaultResultSetReaders.BLOB_READER;
import static org.codefilarete.stalactite.sql.binder.DefaultResultSetReaders.BOOLEAN_PRIMITIVE_READER;
import static org.codefilarete.stalactite.sql.binder.DefaultResultSetReaders.BYTES_READER;
import static org.codefilarete.stalactite.sql.binder.DefaultResultSetReaders.BYTE_PRIMITIVE_READER;
import static org.codefilarete.stalactite.sql.binder.DefaultResultSetReaders.DATE_SQL_READER;
import static org.codefilarete.stalactite.sql.binder.DefaultResultSetReaders.DOUBLE_PRIMITIVE_READER;
import static org.codefilarete.stalactite.sql.binder.DefaultResultSetReaders.FLOAT_PRIMITIVE_READER;
import static org.codefilarete.stalactite.sql.binder.DefaultResultSetReaders.INTEGER_PRIMITIVE_READER;
import static org.codefilarete.stalactite.sql.binder.DefaultResultSetReaders.LONG_PRIMITIVE_READER;
import static org.codefilarete.stalactite.sql.binder.DefaultResultSetReaders.STRING_READER;
import static org.codefilarete.stalactite.sql.binder.DefaultResultSetReaders.TIMESTAMP_READER;

/**
 * Default {@link ParameterBinder}s mapped to methods of {@link ResultSet} and {@link PreparedStatement} 
 * 
 * @author Guillaume Mary
 */
public final class DefaultParameterBinders {
	
	/* Implementation note: although primitive binders are parameterized with generics they are not wrapped by a NullAwareParameterBinder
	 * so they'll throw an exception or convert Object-typed value passed if null is writen or read.
	 */
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getLong(String)} and {@link PreparedStatement#setLong(int, long)}.
	 */
	public static final ParameterBinder<Long> LONG_PRIMITIVE_BINDER = new LambdaParameterBinder<>(LONG_PRIMITIVE_READER, LONG_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getLong(String)} and {@link PreparedStatement#setLong(int, long)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	public static final ParameterBinder<Long> LONG_BINDER = new NullAwareParameterBinder<>(LONG_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getInt(String)} and {@link PreparedStatement#setInt(int, int)}.
	 */
	public static final ParameterBinder<Integer> INTEGER_PRIMITIVE_BINDER = new LambdaParameterBinder<>(INTEGER_PRIMITIVE_READER, INTEGER_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getInt(String)} and {@link PreparedStatement#setInt(int, int)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	public static final ParameterBinder<Integer> INTEGER_BINDER = new NullAwareParameterBinder<>(INTEGER_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getByte(String)} and {@link PreparedStatement#setByte(int, byte)}.
	 */
	public static final ParameterBinder<Byte> BYTE_PRIMITIVE_BINDER = new LambdaParameterBinder<>(BYTE_PRIMITIVE_READER, BYTE_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getByte(String)} and {@link PreparedStatement#setByte(int, byte)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	public static final ParameterBinder<Byte> BYTE_BINDER = new NullAwareParameterBinder<>(BYTE_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBytes(String)} and {@link PreparedStatement#setBytes(int, byte[])}.
	 */
	public static final ParameterBinder<byte[]> BYTES_BINDER = new NullAwareParameterBinder<>(new LambdaParameterBinder<>(BYTES_READER, BYTES_WRITER));
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getDouble(String)} and {@link PreparedStatement#setDouble(int, double)}.
	 */
	public static final ParameterBinder<Double> DOUBLE_PRIMITIVE_BINDER = new LambdaParameterBinder<>(DOUBLE_PRIMITIVE_READER, DOUBLE_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getDouble(String)} and {@link PreparedStatement#setDouble(int, double)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	public static final ParameterBinder<Double> DOUBLE_BINDER = new NullAwareParameterBinder<>(DOUBLE_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getFloat(String)} and {@link PreparedStatement#setFloat(int, float)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	public static final ParameterBinder<Float> FLOAT_PRIMITIVE_BINDER = new LambdaParameterBinder<>(FLOAT_PRIMITIVE_READER, FLOAT_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getFloat(String)} and {@link PreparedStatement#setFloat(int, float)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	public static final ParameterBinder<Float> FLOAT_BINDER = new NullAwareParameterBinder<>(FLOAT_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBigDecimal(String)} and {@link PreparedStatement#setBigDecimal(int, BigDecimal)}.
	 */
	public static final ParameterBinder<BigDecimal> BIGDECIMAL_BINDER = new LambdaParameterBinder<>(BIGDECIMAL_READER, BIGDECIMAL_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBoolean(String)} and {@link PreparedStatement#setBoolean(int, boolean)}.
	 */
	public static final ParameterBinder<Boolean> BOOLEAN_PRIMITIVE_BINDER = new LambdaParameterBinder<>(BOOLEAN_PRIMITIVE_READER, BOOLEAN_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBoolean(String)} and {@link PreparedStatement#setBoolean(int, boolean)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	public static final ParameterBinder<Boolean> BOOLEAN_BINDER = new NullAwareParameterBinder<>(BOOLEAN_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getDate(String)} and {@link PreparedStatement#setDate(int, Date)}.
	 * For common usage, prefer {@link DateBinder} because it uses {@link java.util.Date}
	 * @see DateBinder
	 */
	public static final ParameterBinder<Date> DATE_SQL_BINDER = new LambdaParameterBinder<>(DATE_SQL_READER, DATE_SQL_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getTimestamp(String)} and {@link PreparedStatement#setTimestamp(int, Timestamp)}.
	 */
	public static final ParameterBinder<Timestamp> TIMESTAMP_BINDER = new LambdaParameterBinder<>(TIMESTAMP_READER, TIMESTAMP_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getString(String)} and {@link PreparedStatement#setString(int, String)}.
	 */
	public static final ParameterBinder<String> STRING_BINDER = new LambdaParameterBinder<>(STRING_READER, STRING_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBinaryStream(String)} and {@link PreparedStatement#setBinaryStream(int, InputStream)}.
	 * @see #BLOB_BINDER
	 */
	public static final ParameterBinder<InputStream> BINARYSTREAM_BINDER = new LambdaParameterBinder<>(BINARYSTREAM_READER, BINARYSTREAM_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBlob(String)} and {@link PreparedStatement#setBlob(int, Blob)}.
	 * 
	 * @see #BINARYSTREAM_BINDER
	 */
	public static final ParameterBinder<Blob> BLOB_BINDER = new NullAwareParameterBinder<>(new LambdaParameterBinder<>(BLOB_READER, BLOB_WRITER));
	
	/**
	 * {@link ParameterBinder} for {@link java.util.Date}
	 */
	public static final ParameterBinder<java.util.Date> DATE_BINDER = new NullAwareParameterBinder<>(new DateBinder());
	
	/**
	 * {@link ParameterBinder} for {@link java.time.LocalDate}
	 */
	public static final ParameterBinder<LocalDate> LOCALDATE_BINDER = new NullAwareParameterBinder<>(new LocalDateBinder());
	
	/**
	 * {@link ParameterBinder} for {@link java.time.LocalDateTime}
	 */
	public static final ParameterBinder<LocalDateTime> LOCALDATETIME_BINDER = new NullAwareParameterBinder<>(new LocalDateTimeBinder());
	
	/**
	 * {@link ParameterBinder} for {@link java.time.LocalTime}
	 */
	public static final ParameterBinder<LocalTime> LOCALTIME_BINDER = new NullAwareParameterBinder<>(new LocalTimeBinder());
	
	/**
	 * {@link ParameterBinder} for {@link java.time.ZoneId}.
	 * It may have little purpose alone, but has its interest with {@link java.time.ZonedDateTime}.
	 */
	public static final ParameterBinder<ZoneId> ZONEID_BINDER = new NullAwareParameterBinder<>(new ZoneIdBinder());
	
	/**
	 * {@link ParameterBinder} for {@link UUID}.
	 */
	public static final ParameterBinder<UUID> UUID_BINDER = new NullAwareParameterBinder<>(new UUIDParameterBinder());
	
	/**
	 * {@link ParameterBinder} for {@link Path}.
	 */
	public static final ParameterBinder<Path> PATH_BINDER = new NullAwareParameterBinder<>(new PathBinder());
	
	/**
	 * {@link ParameterBinder} for {@link File}.
	 */
	public static final ParameterBinder<File> FILE_BINDER = new NullAwareParameterBinder<>(new FileBinder());
	
	private DefaultParameterBinders() {
		// Class for constants
	}
}
