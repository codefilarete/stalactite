package org.gama.stalactite.sql.binder;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link ParameterBinder} dedicated to {@link File} : uses {@link ResultSet#getString(int)} and {@link PreparedStatement#setString(int, String)}
 *
 * @author Guillaume Mary
 */
public class FileBinder implements ParameterBinder<File> {
	
	@Override
	public File doGet(ResultSet resultSet, String columnName) throws SQLException {
		return new File(resultSet.getString(columnName));
	}
	
	@Override
	public void set(PreparedStatement statement, int valueIndex, File value) throws SQLException {
		statement.setString(valueIndex, value.toString());
	}
}
