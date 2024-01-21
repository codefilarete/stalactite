package org.codefilarete.stalactite.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.function.SerializableThrowingBiFunction;

/**
 * Reader of a particular column (by its name) from a {@link java.sql.ResultSet}
 * 
 * @author Guillaume Mary
 */
public class SingleColumnReader<C> implements ColumnReader<C> {
	
	private final String columnName;
	
	private final ResultSetReader<C> reader;
	
	public SingleColumnReader(String columnName, SerializableThrowingBiFunction<ResultSet, String, C, SQLException> resultSetGetter) {
		this(columnName, ResultSetReader.ofMethodReference(resultSetGetter));
	}
	
	public SingleColumnReader(String columnName, ResultSetReader<C> reader) {
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
	public C read(ResultSet resultSet) {
		return reader.get(resultSet, columnName);
	}
	
	@Override
	public SingleColumnReader<C> copyWithAliases(Function<String, String> columnMapping) {
		return new SingleColumnReader<>(columnMapping.apply(columnName), reader);
	}
}
