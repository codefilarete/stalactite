package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

/**
 * Specialization of {@link JdbcTypePreparedStatementWriter} for {@link ZonedDateTime} that converts it to {@link OffsetDateTime} to make it handled
 * by HSQLDB.
 * This is necessary because HSQLDB does not support the {@link ZonedDateTime} type directly. Instead, it supports the {@link OffsetDateTime} type.
 *  
 * @author Guillaume Mary
 */
public class ZonedDateTimePreparedStatementWriter extends JdbcTypePreparedStatementWriter<ZonedDateTime> {
	
	public ZonedDateTimePreparedStatementWriter() {
		super(ZonedDateTime.class, JDBCType.TIMESTAMP_WITH_TIMEZONE);
	}
	
	@Override
	public void set(PreparedStatement preparedStatement, int valueIndex, ZonedDateTime value) throws SQLException {
		preparedStatement.setObject(valueIndex, value.toOffsetDateTime(), JDBCType.TIMESTAMP_WITH_TIMEZONE);
	}
}