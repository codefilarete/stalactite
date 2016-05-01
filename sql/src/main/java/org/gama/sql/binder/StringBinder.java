package org.gama.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link AbstractParameterBinder} dédié aux Strings
 * 
 * @author mary
 */
public class StringBinder extends AbstractParameterBinder<String> {

	@Override
	public void setNotNull(int valueIndex, String value, PreparedStatement statement) throws SQLException {
		statement.setString(valueIndex, value);
	}

	@Override
	public String getNotNull(String columnName, ResultSet resultSet) throws SQLException {
		return resultSet.getString(columnName);
	}
}
