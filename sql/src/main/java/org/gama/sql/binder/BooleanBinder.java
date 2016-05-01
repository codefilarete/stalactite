package org.gama.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link AbstractParameterBinder} dédié aux Integers
 * 
 * @author mary
 */
public class BooleanBinder extends AbstractParameterBinder<Boolean> {

	@Override
	public void setNotNull(int valueIndex, Boolean value, PreparedStatement statement) throws SQLException {
		statement.setBoolean(valueIndex, value);
	}

	@Override
	public Boolean getNotNull(String columnName, ResultSet resultSet) throws SQLException {
		return resultSet.getBoolean(columnName);
	}
}
