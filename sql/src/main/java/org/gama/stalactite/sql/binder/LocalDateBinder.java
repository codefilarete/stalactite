package org.gama.stalactite.sql.binder;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

/**
 * {@link ParameterBinder} dedicated to {@link LocalDate} : uses {@link ResultSet#getDate(String)} and {@link PreparedStatement#setDate(int, Date)}
 *
 * @author Guillaume Mary
 */
public class LocalDateBinder implements ParameterBinder<LocalDate> {
	
	@Override
	public LocalDate get(ResultSet resultSet, String columnName) throws SQLException {
		return resultSet.getDate(columnName).toLocalDate();
	}
	
	@Override
	public void set(PreparedStatement statement, int valueIndex, LocalDate value) throws SQLException {
		statement.setDate(valueIndex, java.sql.Date.valueOf(value));
	}
}
