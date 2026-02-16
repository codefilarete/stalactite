package org.codefilarete.stalactite.sql.hsqldb.statement.binder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

/**
 * Specialization of {@link JdbcTypeResultSetReader} for {@link ZonedDateTime} that converts it to {@link OffsetDateTime} to make it handled by HSQLDB.
 * This is necessary because HSQLDB does not support the {@link ZonedDateTime} type directly. Instead, it supports the {@link OffsetDateTime} type.
 * 
 * @author Guillaume Mary
 */
public class ZonedDateTimeResultSetReader extends JdbcTypeResultSetReader<ZonedDateTime> {
	
	public ZonedDateTimeResultSetReader() {
		super(ZonedDateTime.class);
	}
	
	@Override
	public ZonedDateTime doGet(ResultSet resultSet, String columnName) throws SQLException {
		OffsetDateTime offsetDateTime = resultSet.getObject(columnName, OffsetDateTime.class);
		return offsetDateTime.toZonedDateTime();
	}
}
