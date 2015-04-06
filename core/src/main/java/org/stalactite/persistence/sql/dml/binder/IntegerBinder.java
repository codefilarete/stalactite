package org.stalactite.persistence.sql.dml.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nonnull;

/**
 * {@link AbstractParameterBinder} dédié aux Integers
 * 
 * @author mary
 */
public class IntegerBinder extends AbstractParameterBinder<Integer> {

	@Override
	public void setNotNull(int valueIndex, @Nonnull Integer value, PreparedStatement statement) throws SQLException {
		statement.setInt(valueIndex, value);
	}

	@Override
	public Integer getNotNull(String columnName, ResultSet resultSet) throws SQLException {
		return resultSet.getInt(columnName);
	}
}
