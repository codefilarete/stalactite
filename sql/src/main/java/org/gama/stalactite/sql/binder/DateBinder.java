package org.gama.stalactite.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

/**
 * {@link ParameterBinder} dedicated to {@link Date} : uses {@link ResultSet#getTimestamp(String)} and {@link PreparedStatement#setTimestamp(int, Timestamp)}
 *
 * @author Guillaume Mary
 */
public class DateBinder implements ParameterBinder<Date> {
	
	@Override
	public Date doGet(ResultSet resultSet, String columnName) throws SQLException {
		return new Date(resultSet.getTimestamp(columnName).getTime());
	}
	
	@Override
	public void set(PreparedStatement statement, int valueIndex, Date value) throws SQLException {
		statement.setTimestamp(valueIndex, new Timestamp(value.getTime()));
	}
}
