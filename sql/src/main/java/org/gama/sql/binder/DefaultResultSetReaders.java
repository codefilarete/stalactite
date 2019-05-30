package org.gama.sql.binder;

import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URL;
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
public interface DefaultResultSetReaders {
	
	/* Implementation note: although primitive binders are parameterized with generics they are not wrapped by a NullAwareResultSetReader
	 * so they'll throw an exception or convert Object-typed value passed if null is writen or read.
	 */
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getLong(String)}.
	 */
	ResultSetReader<Long> LONG_PRIMITIVE_READER = ResultSet::getLong;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getLong(String)}.
	 * Wrapped into a {@link NullAwareResultSetReader} to manage type boxing and unboxing.
	 */
	ResultSetReader<Long> LONG_READER = new NullAwareResultSetReader<>(LONG_PRIMITIVE_READER);
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getInt(String)}.
	 */
	ResultSetReader<Integer> INTEGER_PRIMITIVE_READER = ResultSet::getInt;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getInt(String)}.
	 * Wrapped into a {@link NullAwareResultSetReader} to manage type boxing and unboxing.
	 */
	ResultSetReader<Integer> INTEGER_READER = new NullAwareResultSetReader<>(INTEGER_PRIMITIVE_READER);
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getByte(String)}.
	 */
	ResultSetReader<Byte> BYTE_PRIMITIVE_READER = ResultSet::getByte;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getByte(String)}.
	 * Wrapped into a {@link NullAwareResultSetReader} to manage type boxing and unboxing.
	 */
	ResultSetReader<Byte> BYTE_READER = new NullAwareResultSetReader<>(BYTE_PRIMITIVE_READER);
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getBytes(String)}.
	 */
	ResultSetReader<byte[]> BYTES_READER = ResultSet::getBytes;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getDouble(String)}.
	 */
	ResultSetReader<Double> DOUBLE_PRIMITIVE_READER = ResultSet::getDouble;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getDouble(String)}.
	 * Wrapped into a {@link NullAwareResultSetReader} to manage type boxing and unboxing.
	 */
	ResultSetReader<Double> DOUBLE_READER = new NullAwareResultSetReader<>(DOUBLE_PRIMITIVE_READER);
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getFloat(String)}.
	 */
	ResultSetReader<Float> FLOAT_PRIMITIVE_READER = ResultSet::getFloat;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getFloat(String)}.
	 * Wrapped into a {@link NullAwareResultSetReader} to manage type boxing and unboxing.
	 */
	ResultSetReader<Float> FLOAT_READER = new NullAwareResultSetReader<>(FLOAT_PRIMITIVE_READER);
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getBigDecimal(String)}.
	 */
	ResultSetReader<BigDecimal> BIGDECIMAL_READER = new NullAwareResultSetReader<>(ResultSet::getBigDecimal);
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getBoolean(String)}.
	 */
	ResultSetReader<Boolean> BOOLEAN_PRIMITIVE_READER = ResultSet::getBoolean;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getBoolean(String)}.
	 * Wrapped into a {@link NullAwareResultSetReader} to manage type boxing and unboxing.
	 */
	ResultSetReader<Boolean> BOOLEAN_READER = new NullAwareResultSetReader<>(BOOLEAN_PRIMITIVE_READER);
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getDate(String)}.
	 * For common usage, prefer {@link DateBinder} because it uses {@link java.util.Date}
	 * @see DateBinder
	 */
	ResultSetReader<Date> DATE_SQL_READER = ResultSet::getDate;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getTimestamp(String)}.
	 */
	ResultSetReader<Timestamp> TIMESTAMP_READER = ResultSet::getTimestamp;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getString(String)}.
	 */
	ResultSetReader<String> STRING_READER = ResultSet::getString;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getBinaryStream(String)}.
	 * @see DerbyParameterBinders#BINARYSTREAM_BINDER
	 * @see HSQLDBParameterBinders#BINARYSTREAM_BINDER
	 */
	ResultSetReader<InputStream> BINARYSTREAM_READER = ResultSet::getBinaryStream;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getURL(String)}.
	 */
	ResultSetReader<URL> URL_READER = ResultSet::getURL;
	
	/**
	 * {@link ResultSetReader} for {@link ResultSet#getBlob(String)}.
	 */
	ResultSetReader<Blob> BLOB_READER = ResultSet::getBlob;
	
	/**
	 * {@link ResultSetReader} for {@link java.util.Date}
	 */
	ResultSetReader<java.util.Date> DATE_READER = new NullAwareResultSetReader<>(new DateBinder());
	
	/**
	 * {@link ResultSetReader} for {@link java.time.LocalDate}
	 */
	ResultSetReader<LocalDate> LOCALDATE_READER = new NullAwareResultSetReader<>(new LocalDateBinder());
	
	/**
	 * {@link ResultSetReader} for {@link java.time.LocalDateTime}
	 */
	ResultSetReader<LocalDateTime> LOCALDATETIME_READER = new NullAwareResultSetReader<>(new LocalDateTimeBinder());
	
}
