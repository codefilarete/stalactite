package org.gama.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nonnull;

/**
 * {@link AbstractParameterBinder} dédié aux Floats
 * 
 * @author mary
 */
public class FloatBinder extends AbstractParameterBinder<Float> {

	@Override
	public void setNotNull(int valueIndex, @Nonnull Float value, PreparedStatement statement) throws SQLException {
		statement.setFloat(valueIndex, value);
	}

	@Override
	public Float getNotNull(String columnName, ResultSet resultSet) throws SQLException {
		return resultSet.getFloat(columnName);
	}
}
