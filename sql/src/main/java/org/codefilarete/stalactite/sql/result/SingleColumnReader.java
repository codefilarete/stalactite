package org.codefilarete.stalactite.sql.result;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.util.function.Function;

import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;

/**
 * Reader of a particular column (by its name) from a {@link java.sql.ResultSet}
 * 
 * @author Guillaume Mary
 */
public class SingleColumnReader<C> implements ColumnReader<C> {
	
	private final String columnName;
	
	private final ResultSetReader<C> reader;
	
	public SingleColumnReader(@Nonnull String columnName, @Nonnull ResultSetReader<C> reader) {
		this.columnName = columnName;
		this.reader = reader;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public ResultSetReader<C> getReader() {
		return reader;
	}
	
	@Override
	public C read(@Nonnull ResultSet resultSet) {
		return reader.get(resultSet, columnName);
	}
	
	@Override
	public SingleColumnReader<C> copyWithAliases(Function<String, String> columnMapping) {
		return new SingleColumnReader<>(columnMapping.apply(columnName), reader);
	}
}
