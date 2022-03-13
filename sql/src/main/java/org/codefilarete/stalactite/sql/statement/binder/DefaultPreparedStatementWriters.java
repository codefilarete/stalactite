package org.codefilarete.stalactite.sql.statement.binder;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Blob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Default {@link PreparedStatementWriter}s mapped to methods of {@link PreparedStatement}
 * 
 * @author Guillaume Mary
 */
public final class DefaultPreparedStatementWriters {
	
	/* Implementation note: although primitive binders are parameterized with generics they are not wrapped by a NullAwarePreparedStatementWriter
	 * so they'll throw an exception or convert Object-typed value passed if null is writen or read.
	 */
	
	/** Writer straightly bound to {@link PreparedStatement#setLong(int, long)} */
	private static final PreparedStatementWriter<Long> SET_LONG_WRITER = PreparedStatement::setLong;
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setLong(int, long)}.
	 */
	public static final PreparedStatementWriter<Long> LONG_PRIMITIVE_WRITER = new NullSafeguardPreparedStatementWriter<>(SET_LONG_WRITER);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setLong(int, long)}.
	 * Wrapped into a {@link NullAwarePreparedStatementWriter} to manage type boxing and unboxing.
	 */
	public static final PreparedStatementWriter<Long> LONG_WRITER = new NullAwarePreparedStatementWriter<>(SET_LONG_WRITER);
	
	/** Writer straightly bound to {@link PreparedStatement#setInt(int, int)} */
	private static final PreparedStatementWriter<Integer> SET_INT_WRITER = PreparedStatement::setInt;
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setInt(int, int)}.
	 */
	public static final PreparedStatementWriter<Integer> INTEGER_PRIMITIVE_WRITER = new NullSafeguardPreparedStatementWriter<>(SET_INT_WRITER);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setInt(int, int)}.
	 * Wrapped into a {@link NullAwarePreparedStatementWriter} to manage type boxing and unboxing.
	 */
	public static final PreparedStatementWriter<Integer> INTEGER_WRITER = new NullAwarePreparedStatementWriter<>(SET_INT_WRITER);
	
	/** Writer straightly bound to {@link PreparedStatement#setByte(int, byte)} */
	private static final PreparedStatementWriter<Byte> SET_BYTE_WRITER = PreparedStatement::setByte;
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setByte(int, byte)}.
	 */
	public static final PreparedStatementWriter<Byte> BYTE_PRIMITIVE_WRITER = new NullSafeguardPreparedStatementWriter<>(SET_BYTE_WRITER);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setByte(int, byte)}.
	 * Wrapped into a {@link NullAwarePreparedStatementWriter} to manage type boxing and unboxing.
	 */
	public static final PreparedStatementWriter<Byte> BYTE_WRITER = new NullAwarePreparedStatementWriter<>(SET_BYTE_WRITER);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setBytes(int, byte[])}.
	 */
	public static final PreparedStatementWriter<byte[]> BYTES_WRITER = new NullAwarePreparedStatementWriter<>(PreparedStatement::setBytes);
	
	/** Writer straightly bound to {@link PreparedStatement#setDouble(int, double)} */
	private static final PreparedStatementWriter<Double> SET_DOUBLE_WRITER = PreparedStatement::setDouble;
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setDouble(int, double)}.
	 */
	public static final PreparedStatementWriter<Double> DOUBLE_PRIMITIVE_WRITER = new NullSafeguardPreparedStatementWriter<>(SET_DOUBLE_WRITER);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setDouble(int, double)}.
	 * Wrapped into a {@link NullAwarePreparedStatementWriter} to manage type boxing and unboxing.
	 */
	public static final PreparedStatementWriter<Double> DOUBLE_WRITER = new NullAwarePreparedStatementWriter<>(SET_DOUBLE_WRITER);
	
	/** Writer straightly bound to {@link PreparedStatement#setFloat(int, float)} */
	private static final PreparedStatementWriter<Float> SET_FLOAT_WRITER = PreparedStatement::setFloat;
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setFloat(int, float)}.
	 * Wrapped into a {@link NullAwarePreparedStatementWriter} to manage type boxing and unboxing.
	 */
	public static final PreparedStatementWriter<Float> FLOAT_PRIMITIVE_WRITER = new NullSafeguardPreparedStatementWriter<>(SET_FLOAT_WRITER);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setFloat(int, float)}.
	 * Wrapped into a {@link NullAwarePreparedStatementWriter} to manage type boxing and unboxing.
	 */
	public static final PreparedStatementWriter<Float> FLOAT_WRITER = new NullAwarePreparedStatementWriter<>(SET_FLOAT_WRITER);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setBigDecimal(int, BigDecimal)}.
	 */
	public static final PreparedStatementWriter<BigDecimal> BIGDECIMAL_WRITER = new NullAwarePreparedStatementWriter<>(PreparedStatement::setBigDecimal);
	
	/** Writer straightly bound to {@link PreparedStatement#setBoolean(int, boolean)} */
	private static final PreparedStatementWriter<Boolean> SET_BOOLEAN_WRITER = PreparedStatement::setBoolean;
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setBoolean(int, boolean)}.
	 */
	public static final PreparedStatementWriter<Boolean> BOOLEAN_PRIMITIVE_WRITER = new NullSafeguardPreparedStatementWriter<>(SET_BOOLEAN_WRITER);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setBoolean(int, boolean)}.
	 * Wrapped into a {@link NullAwarePreparedStatementWriter} to manage type boxing and unboxing.
	 */
	public static final PreparedStatementWriter<Boolean> BOOLEAN_WRITER = new NullAwarePreparedStatementWriter<>(SET_BOOLEAN_WRITER);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setDate(int, Date)}.
	 * For common usage, prefer {@link DateBinder} because it uses {@link java.util.Date}
	 * @see DateBinder
	 */
	public static final PreparedStatementWriter<Date> DATE_SQL_WRITER = new NullAwarePreparedStatementWriter<>(PreparedStatement::setDate);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setTimestamp(int, Timestamp)}.
	 */
	public static final PreparedStatementWriter<Timestamp> TIMESTAMP_WRITER = new NullAwarePreparedStatementWriter<>(PreparedStatement::setTimestamp);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setString(int, String)}.
	 */
	public static final PreparedStatementWriter<String> STRING_WRITER = new NullAwarePreparedStatementWriter<>(PreparedStatement::setString);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setBinaryStream(int, InputStream)}.
	 */
	public static final PreparedStatementWriter<InputStream> BINARYSTREAM_WRITER = new NullAwarePreparedStatementWriter<>(PreparedStatement::setBinaryStream);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setBlob(int, Blob)}.
	 */
	public static final PreparedStatementWriter<Blob> BLOB_WRITER = new NullAwarePreparedStatementWriter<>(PreparedStatement::setBlob);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setBlob(int, InputStream)}.
	 */
	public static final PreparedStatementWriter<InputStream> BLOB_INPUTSTREAM_WRITER = new NullAwarePreparedStatementWriter<>(PreparedStatement::setBinaryStream);
	
	/**
	 * {@link PreparedStatementWriter} for {@link java.util.Date}
	 */
	public static final PreparedStatementWriter<java.util.Date> DATE_WRITER = new NullAwarePreparedStatementWriter<>(new DateBinder());
	
	/**
	 * {@link PreparedStatementWriter} for {@link java.time.LocalDate}
	 */
	public static final PreparedStatementWriter<LocalDate> LOCALDATE_WRITER = new NullAwarePreparedStatementWriter<>(new LocalDateBinder());
	
	/**
	 * {@link PreparedStatementWriter} for {@link java.time.LocalDateTime}
	 */
	public static final PreparedStatementWriter<LocalDateTime> LOCALDATETIME_WRITER = new NullAwarePreparedStatementWriter<>(new LocalDateTimeBinder());
	
	/**
	 * {@link PreparedStatementWriter} for {@link UUID}
	 */
	public static final PreparedStatementWriter<UUID> UUID_WRITER = new NullAwarePreparedStatementWriter<>(new UUIDParameterBinder());
	
	/**
	 * {@link PreparedStatementWriter} for {@link Path}
	 */
	public static final PreparedStatementWriter<Path> PATH_WRITER = new NullAwarePreparedStatementWriter<>(new PathBinder());
	
	private DefaultPreparedStatementWriters() {
		// Class for constants
	}
}
