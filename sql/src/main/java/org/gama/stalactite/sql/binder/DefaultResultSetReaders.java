package org.gama.stalactite.sql.binder;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Default {@link ResultSetReader}s mapped to methods of {@link ResultSet}
 *
 * @author Guillaume Mary
 */
public final class DefaultResultSetReaders {
	
	/* Implementation note: although primitive binders are parameterized with generics they are not wrapped by a NullAwareResultSetReader
	 * so they'll throw an exception or convert Object-typed value passed if null is writen or read.
	 */
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getLong(String)}.
	 */
	public static final ResultSetReader<Long> LONG_PRIMITIVE_READER = ResultSet::getLong;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getLong(String)}.
	 * Wrapped into a {@link NullAwareResultSetReader} to manage type boxing and unboxing.
	 */
	public static final ResultSetReader<Long> LONG_READER = new NullAwareResultSetReader<>(LONG_PRIMITIVE_READER);
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getInt(String)}.
	 */
	public static final ResultSetReader<Integer> INTEGER_PRIMITIVE_READER = ResultSet::getInt;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getInt(String)}.
	 * Wrapped into a {@link NullAwareResultSetReader} to manage type boxing and unboxing.
	 */
	public static final ResultSetReader<Integer> INTEGER_READER = new NullAwareResultSetReader<>(INTEGER_PRIMITIVE_READER);
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getByte(String)}.
	 */
	public static final ResultSetReader<Byte> BYTE_PRIMITIVE_READER = ResultSet::getByte;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getByte(String)}.
	 * Wrapped into a {@link NullAwareResultSetReader} to manage type boxing and unboxing.
	 */
	public static final ResultSetReader<Byte> BYTE_READER = new NullAwareResultSetReader<>(BYTE_PRIMITIVE_READER);
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getBytes(String)}.
	 */
	public static final ResultSetReader<byte[]> BYTES_READER = ResultSet::getBytes;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getDouble(String)}.
	 */
	public static final ResultSetReader<Double> DOUBLE_PRIMITIVE_READER = ResultSet::getDouble;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getDouble(String)}.
	 * Wrapped into a {@link NullAwareResultSetReader} to manage type boxing and unboxing.
	 */
	public static final ResultSetReader<Double> DOUBLE_READER = new NullAwareResultSetReader<>(DOUBLE_PRIMITIVE_READER);
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getFloat(String)}.
	 */
	public static final ResultSetReader<Float> FLOAT_PRIMITIVE_READER = ResultSet::getFloat;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getFloat(String)}.
	 * Wrapped into a {@link NullAwareResultSetReader} to manage type boxing and unboxing.
	 */
	public static final ResultSetReader<Float> FLOAT_READER = new NullAwareResultSetReader<>(FLOAT_PRIMITIVE_READER);
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getBigDecimal(String)}.
	 */
	public static final ResultSetReader<BigDecimal> BIGDECIMAL_READER = new NullAwareResultSetReader<>(ResultSet::getBigDecimal);
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getBoolean(String)}.
	 */
	public static final ResultSetReader<Boolean> BOOLEAN_PRIMITIVE_READER = ResultSet::getBoolean;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getBoolean(String)}.
	 * Wrapped into a {@link NullAwareResultSetReader} to manage type boxing and unboxing.
	 */
	public static final ResultSetReader<Boolean> BOOLEAN_READER = new NullAwareResultSetReader<>(BOOLEAN_PRIMITIVE_READER);
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getDate(String)}.
	 * For common usage, prefer {@link DateBinder} because it uses {@link java.util.Date}
	 * @see DateBinder
	 */
	public static final ResultSetReader<Date> DATE_SQL_READER = ResultSet::getDate;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getTimestamp(String)}.
	 */
	public static final ResultSetReader<Timestamp> TIMESTAMP_READER = ResultSet::getTimestamp;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getString(String)}.
	 */
	public static final ResultSetReader<String> STRING_READER = ResultSet::getString;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getBinaryStream(String)}.
	 * @see DerbyParameterBinders#BINARYSTREAM_BINDER
	 * @see HSQLDBParameterBinders#BINARYSTREAM_BINDER
	 */
	public static final ResultSetReader<InputStream> BINARYSTREAM_READER = ResultSet::getBinaryStream;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getBlob(String)}.
	 */
	public static final ResultSetReader<Blob> BLOB_READER = ResultSet::getBlob;
	
	/**
	 * {@link ResultSetReader} for {@link java.util.Date}
	 */
	public static final ResultSetReader<java.util.Date> DATE_READER = new NullAwareResultSetReader<>(new DateBinder());
	
	/**
	 * {@link ResultSetReader} for {@link java.time.LocalDate}
	 */
	public static final ResultSetReader<LocalDate> LOCALDATE_READER = new NullAwareResultSetReader<>(new LocalDateBinder());
	
	/**
	 * {@link ResultSetReader} for {@link java.time.LocalDateTime}
	 */
	public static final ResultSetReader<LocalDateTime> LOCALDATETIME_READER = new NullAwareResultSetReader<>(new LocalDateTimeBinder());
	
	/**
	 * {@link ResultSetReader} for {@link Object}
	 */
	public static final ResultSetReader<Object> OBJECT_READER = ResultSet::getObject;
	
	private DefaultResultSetReaders() {
		// Class for constants
	}
}
