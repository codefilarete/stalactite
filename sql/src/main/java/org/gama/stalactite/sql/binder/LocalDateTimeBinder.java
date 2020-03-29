package org.gama.stalactite.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * {@link ParameterBinder} dedicated to {@link LocalDateTime} : uses {@link ResultSet#getTimestamp(String)} and {@link PreparedStatement#setTimestamp(int, Timestamp)}
 *
 * @author Guillaume Mary
 */
public class LocalDateTimeBinder implements ParameterBinder<LocalDateTime> {
	
	@Override
	public LocalDateTime doGet(ResultSet resultSet, String columnName) throws SQLException {
		return resultSet.getTimestamp(columnName).toLocalDateTime();
	}
	
	@Override
	public void set(PreparedStatement statement, int valueIndex, LocalDateTime value) throws SQLException {
		statement.setTimestamp(valueIndex, java.sql.Timestamp.valueOf(value));
	}
}
