package org.gama.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

/**
 * {@link ParameterBinder} dedicated to {@link Date} : use {@link ResultSet#getTimestamp(String)} and {@link PreparedStatement#setTimestamp(int, Timestamp)}
 *
 * @author Guillaume Mary
 */
public class DateBinder implements ParameterBinder<Date> {
	
	@Override
	public Date get(String columnName, ResultSet resultSet) throws SQLException {
		return new Date(resultSet.getTimestamp(columnName).getTime());
	}
	
	@Override
	public void set(int valueIndex, Date value, PreparedStatement statement) throws SQLException {
		statement.setTimestamp(valueIndex, new Timestamp(value.getTime()));
	}
}
