package org.gama.sql.binder;

import java.io.InputStream;
import java.net.URL;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.gama.sql.binder.DefaultPreparedStatementWriters.*;
import static org.gama.sql.binder.DefaultResultSetReaders.*;

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
	LambdaParameterBinder<Long> LONG_PRIMITIVE_BINDER = new LambdaParameterBinder<>(LONG_PRIMITIVE_READER, LONG_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getLong(String)} and {@link PreparedStatement#setLong(int, long)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	NullAwareParameterBinder<Long> LONG_BINDER = new NullAwareParameterBinder<>(LONG_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getInt(String)} and {@link PreparedStatement#setInt(int, int)}.
	 */
	LambdaParameterBinder<Integer> INTEGER_PRIMITIVE_BINDER = new LambdaParameterBinder<>(INTEGER_PRIMITIVE_READER, INTEGER_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getInt(String)} and {@link PreparedStatement#setInt(int, int)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	NullAwareParameterBinder<Integer> INTEGER_BINDER = new NullAwareParameterBinder<>(INTEGER_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getDouble(String)} and {@link PreparedStatement#setDouble(int, double)}.
	 */
	LambdaParameterBinder<Double> DOUBLE_PRIMITIVE_BINDER = new LambdaParameterBinder<>(DOUBLE_PRIMITIVE_READER, DOUBLE_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getDouble(String)} and {@link PreparedStatement#setDouble(int, double)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	NullAwareParameterBinder<Double> DOUBLE_BINDER = new NullAwareParameterBinder<>(DOUBLE_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getFloat(String)} and {@link PreparedStatement#setFloat(int, float)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	LambdaParameterBinder<Float> FLOAT_PRIMITIVE_BINDER = new LambdaParameterBinder<>(FLOAT_PRIMITIVE_READER, FLOAT_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getFloat(String)} and {@link PreparedStatement#setFloat(int, float)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	NullAwareParameterBinder<Float> FLOAT_BINDER = new NullAwareParameterBinder<>(FLOAT_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBoolean(String)} and {@link PreparedStatement#setBoolean(int, boolean)}.
	 */
	LambdaParameterBinder<Boolean> BOOLEAN_PRIMITIVE_BINDER = new LambdaParameterBinder<>(BOOLEAN_PRIMITIVE_READER, BOOLEAN_PRIMITIVE_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBoolean(String)} and {@link PreparedStatement#setBoolean(int, boolean)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	NullAwareParameterBinder<Boolean> BOOLEAN_BINDER = new NullAwareParameterBinder<>(BOOLEAN_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getDate(String)} and {@link PreparedStatement#setDate(int, Date)}.
	 * For common usage, prefer {@link DateBinder} because it uses {@link java.util.Date}
	 * @see DateBinder
	 */
	LambdaParameterBinder<Date> DATE_SQL_BINDER = new LambdaParameterBinder<>(DATE_SQL_READER, DATE_SQL_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getTimestamp(String)} and {@link PreparedStatement#setTimestamp(int, Timestamp)}.
	 */
	LambdaParameterBinder<Timestamp> TIMESTAMP_BINDER = new LambdaParameterBinder<>(TIMESTAMP_READER, TIMESTAMP_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getString(String)} and {@link PreparedStatement#setString(int, String)}.
	 */
	LambdaParameterBinder<String> STRING_BINDER = new LambdaParameterBinder<>(STRING_READER, STRING_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBinaryStream(String)} and {@link PreparedStatement#setBinaryStream(int, InputStream)}.
	 * @see DerbyParameterBinders#BINARYSTREAM_BINDER
	 * @see HSQLDBParameterBinders#BINARYSTREAM_BINDER
	 */
	LambdaParameterBinder<InputStream> BINARYSTREAM_BINDER = new LambdaParameterBinder<>(BINARYSTREAM_READER, BINARYSTREAM_WRITER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getURL(int)} and {@link PreparedStatement#setURL(int, URL)}.
	 */
	LambdaParameterBinder<URL> URL_BINDER = new LambdaParameterBinder<>(URL_READER, URL_WRITER);
	
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
	
}
