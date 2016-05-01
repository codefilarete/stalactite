package org.gama.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

/**
 * {@link AbstractParameterBinder} dédié aux Dates
 * 
 * @author mary
 */
public class DateBinder extends AbstractParameterBinder<Date> {

	@Override
	public void setNotNull(int valueIndex, Date value, PreparedStatement statement) throws SQLException {
		statement.setTimestamp(valueIndex, new Timestamp(value.getTime()));
	}

	@Override
	public Date getNotNull(String columnName, ResultSet resultSet) throws SQLException {
		return new Date(resultSet.getTimestamp(columnName).getTime());
	}
}
