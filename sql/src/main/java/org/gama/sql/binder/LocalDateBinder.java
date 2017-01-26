package org.gama.sql.binder;

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
	public LocalDate get(String columnName, ResultSet resultSet) throws SQLException {
		return resultSet.getDate(columnName).toLocalDate();
	}
	
	@Override
	public void set(int valueIndex, LocalDate value, PreparedStatement statement) throws SQLException {
		statement.setDate(valueIndex, java.sql.Date.valueOf(value));
	}
}
