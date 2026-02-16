package org.codefilarete.stalactite.sql.hsqldb.statement.binder;

import java.io.InputStream;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;

import static org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders.BINARYSTREAM_READER;

/**
 * Specialized {@link ParameterBinder}s for HSQLDB.
 * 
 * @author Guillaume Mary
 */
public final class HSQLDBParameterBinders {
	
	/**
	 * Specialization is made by overriding {@link PreparedStatement#setBinaryStream(int, InputStream)} for null values because it fails
	 * with "org.hsqldb.HsqlException: Invalid argument in JDBC call"
	 * HsqlDB 2.3.2
	 */
	public static final ParameterBinder<InputStream> BINARYSTREAM_BINDER = new LambdaParameterBinder<>(BINARYSTREAM_READER, new PreparedStatementWriter<InputStream>() {
		@Override
		public void set(PreparedStatement p, int i, InputStream v) throws SQLException {
			if (v == null) {
				p.setObject(i, null);
			} else {
				p.setBinaryStream(i, v);
			}
		}
		
		@Override
		public Class<InputStream> getType() {
			return InputStream.class;
		}
	});
	
	/**
	 * HSQLDB native support for {@link ZonedDateTime}
	 */
	public static final ParameterBinder<ZonedDateTime> ZONED_DATE_TIME_BINDER = new NullAwareParameterBinder<>(
			new ZonedDateTimeResultSetReader(),
			new ZonedDateTimePreparedStatementWriter());
	
	/**
	 * HSQLDB native support for {@link OffsetDateTime}
	 */
	public static final ParameterBinder<OffsetDateTime> OFFSET_DATE_TIME_BINDER = new NullAwareParameterBinder<>(
			new JdbcTypeResultSetReader<>(OffsetDateTime.class),
			new JdbcTypePreparedStatementWriter<>(OffsetDateTime.class, JDBCType.TIMESTAMP_WITH_TIMEZONE));
	
	private HSQLDBParameterBinders() {}
}
