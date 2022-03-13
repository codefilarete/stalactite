package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * {@link ParameterBinder} dedicated to {@link UUID} : uses {@link ResultSet#getString(int)} and {@link PreparedStatement#setString(int, String)}
 * 
 * @author Guillaume Mary
 */
public class UUIDParameterBinder implements ParameterBinder<UUID> {
	
	@Override
	public void set(PreparedStatement preparedStatement, int valueIndex, UUID value) throws SQLException {
		preparedStatement.setString(valueIndex, value.toString());
	}
	
	@Override
	public UUID doGet(ResultSet resultSet, String columnName) throws SQLException {
		return UUID.fromString(resultSet.getString(columnName));
	}
}
