package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;

/**
 * {@link ParameterBinder} dedicated to {@link ZoneId} : uses {@link ResultSet#getString(int)} and {@link PreparedStatement#setString(int, String)}
 *
 * @author Guillaume Mary
 */
public class ZoneIdBinder implements ParameterBinder<ZoneId> {
	
	@Override
	public Class<ZoneId> getType() {
		return ZoneId.class;
	}
	
	@Override
	public ZoneId doGet(ResultSet resultSet, String columnName) throws SQLException {
		return ZoneId.of(resultSet.getString(columnName));
	}
	
	@Override
	public void set(PreparedStatement statement, int valueIndex, ZoneId value) throws SQLException {
		statement.setString(valueIndex, value.getId());
	}
}
