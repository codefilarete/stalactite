package org.codefilarete.stalactite.sql.statement.binder;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link ParameterBinder} dedicated to {@link Path} : uses {@link ResultSet#getString(int)} and {@link PreparedStatement#setString(int, String)}
 *
 * @author Guillaume Mary
 */
public class PathBinder implements ParameterBinder<Path> {
	
	@Override
	public Class<Path> getType() {
		return Path.class;
	}
	
	@Override
	public Path doGet(ResultSet resultSet, String columnName) throws SQLException {
		return Paths.get(resultSet.getString(columnName));
	}
	
	@Override
	public void set(PreparedStatement statement, int valueIndex, Path value) throws SQLException {
		statement.setString(valueIndex, value.toString());
	}
}
