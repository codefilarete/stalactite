package org.gama.sql.binder;

import java.io.InputStream;
import java.net.URL;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

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
	public static final LambdaParameterBinder<Long> LONG_PRIMITIVE_BINDER = new LambdaParameterBinder<>(ResultSet::getLong, PreparedStatement::setLong);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getLong(String)} and {@link PreparedStatement#setLong(int, long)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	public static final NullAwareParameterBinder<Long> LONG_BINDER = new NullAwareParameterBinder<>(LONG_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getInt(String)} and {@link PreparedStatement#setInt(int, int)}.
	 */
	public static final LambdaParameterBinder<Integer> INTEGER_PRIMITIVE_BINDER = new LambdaParameterBinder<>(ResultSet::getInt, 
			PreparedStatement::setInt);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getInt(String)} and {@link PreparedStatement#setInt(int, int)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	public static final NullAwareParameterBinder<Integer> INTEGER_BINDER = new NullAwareParameterBinder<>(INTEGER_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getDouble(String)} and {@link PreparedStatement#setDouble(int, double)}.
	 */
	public static final LambdaParameterBinder<Double> DOUBLE_PRIMITIVE_BINDER = new LambdaParameterBinder<>(ResultSet::getDouble, 
			PreparedStatement::setDouble);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getDouble(String)} and {@link PreparedStatement#setDouble(int, double)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	public static final NullAwareParameterBinder<Double> DOUBLE_BINDER = new NullAwareParameterBinder<>(DOUBLE_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getFloat(String)} and {@link PreparedStatement#setFloat(int, float)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	public static final LambdaParameterBinder<Float> FLOAT_PRIMITIVE_BINDER = new LambdaParameterBinder<>(ResultSet::getFloat, 
			PreparedStatement::setFloat);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getFloat(String)} and {@link PreparedStatement#setFloat(int, float)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	public static final NullAwareParameterBinder<Float> FLOAT_BINDER = new NullAwareParameterBinder<>(FLOAT_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBoolean(String)} and {@link PreparedStatement#setBoolean(int, boolean)}.
	 */
	public static final LambdaParameterBinder<Boolean> BOOLEAN_PRIMITIVE_BINDER = new LambdaParameterBinder<>(ResultSet::getBoolean, 
			PreparedStatement::setBoolean);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBoolean(String)} and {@link PreparedStatement#setBoolean(int, boolean)}.
	 * Wrapped into a {@link NullAwareParameterBinder} to manage type boxing and unboxing.
	 */
	public static final NullAwareParameterBinder<Boolean> BOOLEAN_BINDER = new NullAwareParameterBinder<>(BOOLEAN_PRIMITIVE_BINDER);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getDate(String)} and {@link PreparedStatement#setDate(int, Date)}.
	 * For common usage, prefer {@link DateBinder} because it uses {@link java.util.Date}
	 * @see DateBinder
	 */
	public static final LambdaParameterBinder<Date> DATE_SQL_BINDER = new LambdaParameterBinder<>(ResultSet::getDate, PreparedStatement::setDate);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getTimestamp(String)} and {@link PreparedStatement#setTimestamp(int, Timestamp)}.
	 */
	public static final LambdaParameterBinder<Timestamp> TIMESTAMP_BINDER = new LambdaParameterBinder<>(ResultSet::getTimestamp, 
			PreparedStatement::setTimestamp);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getString(String)} and {@link PreparedStatement#setString(int, String)}.
	 */
	public static final LambdaParameterBinder<String> STRING_BINDER = new LambdaParameterBinder<>(ResultSet::getString, PreparedStatement::setString);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getBinaryStream(String)} and {@link PreparedStatement#setBinaryStream(int, InputStream)}.
	 * @see DerbyParameterBinders#BINARYSTREAM_BINDER
	 * @see HSQLDBParameterBinders#BINARYSTREAM_BINDER
	 */
	public static final LambdaParameterBinder<InputStream> BINARYSTREAM_BINDER = new LambdaParameterBinder<>(ResultSet::getBinaryStream,
			PreparedStatement::setBinaryStream);
	
	/**
	 * {@link ParameterBinder} for {@link ResultSet#getURL(int)} and {@link PreparedStatement#setURL(int, URL)}.
	 */
	public static final LambdaParameterBinder<URL> URL_BINDER = new LambdaParameterBinder<>(ResultSet::getURL, PreparedStatement::setURL);
	
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
	
	private DefaultParameterBinders() {
	}
}
