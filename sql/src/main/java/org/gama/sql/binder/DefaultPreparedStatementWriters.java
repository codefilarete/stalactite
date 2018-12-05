package org.gama.sql.binder;

import java.io.InputStream;
import java.net.URL;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Default {@link PreparedStatementWriter}s mapped to methods of {@link PreparedStatement}
 * 
 * @author Guillaume Mary
 */
public final class DefaultPreparedStatementWriters {
	
	/* Implementation note: although primitive binders are parameterized with generics they are not wrapped by a NullAwarePreparedStatementWriter
	 * so they'll throw an exception or convert Object-typed value passed if null is writen or read.
	 */
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setLong(int, long)}.
	 */
	public static final PreparedStatementWriter<Long> LONG_PRIMITIVE_WRITER = new NullSafeguardPreparedStatementWriter<>(PreparedStatement::setLong);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setLong(int, long)}.
	 * Wrapped into a {@link NullAwarePreparedStatementWriter} to manage type boxing and unboxing.
	 */
	public static final NullAwarePreparedStatementWriter<Long> LONG_WRITER = new NullAwarePreparedStatementWriter<>(LONG_PRIMITIVE_WRITER);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setInt(int, int)}.
	 */
	public static final PreparedStatementWriter<Integer> INTEGER_PRIMITIVE_WRITER = new NullSafeguardPreparedStatementWriter<>(PreparedStatement::setInt);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setInt(int, int)}.
	 * Wrapped into a {@link NullAwarePreparedStatementWriter} to manage type boxing and unboxing.
	 */
	public static final NullAwarePreparedStatementWriter<Integer> INTEGER_WRITER = new NullAwarePreparedStatementWriter<>(INTEGER_PRIMITIVE_WRITER);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setDouble(int, double)}.
	 */
	public static final PreparedStatementWriter<Double> DOUBLE_PRIMITIVE_WRITER = new NullSafeguardPreparedStatementWriter<>(PreparedStatement::setDouble);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setDouble(int, double)}.
	 * Wrapped into a {@link NullAwarePreparedStatementWriter} to manage type boxing and unboxing.
	 */
	public static final NullAwarePreparedStatementWriter<Double> DOUBLE_WRITER = new NullAwarePreparedStatementWriter<>(DOUBLE_PRIMITIVE_WRITER);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setFloat(int, float)}.
	 * Wrapped into a {@link NullAwarePreparedStatementWriter} to manage type boxing and unboxing.
	 */
	public static final PreparedStatementWriter<Float> FLOAT_PRIMITIVE_WRITER = new NullSafeguardPreparedStatementWriter<>(PreparedStatement::setFloat);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setFloat(int, float)}.
	 * Wrapped into a {@link NullAwarePreparedStatementWriter} to manage type boxing and unboxing.
	 */
	public static final NullAwarePreparedStatementWriter<Float> FLOAT_WRITER = new NullAwarePreparedStatementWriter<>(FLOAT_PRIMITIVE_WRITER);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setBoolean(int, boolean)}.
	 */
	public static final PreparedStatementWriter<Boolean> BOOLEAN_PRIMITIVE_WRITER = new NullSafeguardPreparedStatementWriter<>(PreparedStatement::setBoolean);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setBoolean(int, boolean)}.
	 * Wrapped into a {@link NullAwarePreparedStatementWriter} to manage type boxing and unboxing.
	 */
	public static final NullAwarePreparedStatementWriter<Boolean> BOOLEAN_WRITER = new NullAwarePreparedStatementWriter<>(BOOLEAN_PRIMITIVE_WRITER);
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setDate(int, Date)}.
	 * For common usage, prefer {@link DateBinder} because it uses {@link java.util.Date}
	 * @see DateBinder
	 */
	public static final PreparedStatementWriter<Date> DATE_SQL_WRITER = PreparedStatement::setDate;
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setTimestamp(int, Timestamp)}.
	 */
	public static final PreparedStatementWriter<Timestamp> TIMESTAMP_WRITER = PreparedStatement::setTimestamp;
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setString(int, String)}.
	 */
	public static final PreparedStatementWriter<String> STRING_WRITER = PreparedStatement::setString;
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setBinaryStream(int, InputStream)}.
	 * @see DerbyParameterBinders#BINARYSTREAM_BINDER
	 * @see HSQLDBParameterBinders#BINARYSTREAM_BINDER
	 */
	public static final PreparedStatementWriter<InputStream> BINARYSTREAM_WRITER = PreparedStatement::setBinaryStream;
	
	/**
	 * {@link PreparedStatementWriter} for {@link PreparedStatement#setURL(int, URL)}.
	 */
	public static final PreparedStatementWriter<URL> URL_WRITER = PreparedStatement::setURL;
	
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
	
	
	private DefaultPreparedStatementWriters() {
		// Class for constants
	}
}
