package org.gama.stalactite.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Enum {@link ParameterBinder} based on name of enums
 * 
 * @author Guillaume Mary
 */
public class NameEnumParameterBinder<E extends Enum<E>> extends AbstractEnumParameterBinder<E> {
	
	public NameEnumParameterBinder(Class<E> enumType) {
		super(enumType);
	}
	
	@Override
	public E get(ResultSet resultSet, String columnName) throws SQLException {
		return Enum.valueOf(enumType, resultSet.getString(columnName));
	}
	
	@Override
	public void set(PreparedStatement preparedStatement, int valueIndex, E value) throws SQLException {
		preparedStatement.setString(valueIndex, value.name());
	}
	
}
