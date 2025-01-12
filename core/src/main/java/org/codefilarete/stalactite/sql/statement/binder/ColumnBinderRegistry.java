package org.codefilarete.stalactite.sql.statement.binder;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;

/**
 * Registry of {@link ParameterBinder}s per {@link Column} and {@link Class}.
 * See {@link #register(Column, ParameterBinder)} and {@link #register(Class, ParameterBinder)} to specify binder for a column or type.
 *
 * @author Guillaume Mary
 */
public class ColumnBinderRegistry
		implements ParameterBinderIndex<Column, ParameterBinder>,
		ResultSetReaderRegistry,
		PreparedStatementWriterRegistry {
	
	/**
	 * Registry for {@link Column}s
	 */
	private final Map<Column, ParameterBinder> bindersPerColumn = new HashMap<>();
	
	private final ParameterBinderRegistry parameterBinderRegistry;
	
	public ColumnBinderRegistry() {
		this(new ParameterBinderRegistry());
	}
	
	public ColumnBinderRegistry(ParameterBinderRegistry parameterBinderRegistry) {
		this.parameterBinderRegistry = parameterBinderRegistry;
	}
	
	public <T> void register(Class<T> clazz, ParameterBinder<T> parameterBinder) {
		this.parameterBinderRegistry.register(clazz, parameterBinder);
	}
	
	public <T> void register(Column column, ParameterBinder<T> parameterBinder) {
		ParameterBinder existingBinder = this.bindersPerColumn.get(column);
		if (existingBinder != null && !existingBinder.equals(parameterBinder)) { 
			throw new BindingException("Binder for column " + column + " already exists");
		}
		this.bindersPerColumn.put(column, parameterBinder);
	}
	
	/**
	 * Gives the registered {@link ParameterBinder} for the given type.
	 *
	 * @param clazz a class
	 * @return the registered {@link ParameterBinder} for the given type
	 * @throws BindingException if the binder doesn't exist
	 */
	public <T> ParameterBinder<T> getBinder(Class<T> clazz) {
		return parameterBinderRegistry.getBinder(clazz);
	}
	
	/**
	 * Gives the {@link ParameterBinder} of a column if exists, else gives it for the Java type of the {@link Column}
	 *
	 * @param column any non null {@link Column}
	 * @return the binder for the column or for its Java type
	 * @throws BindingException if the binder doesn't exist
	 */
	@Override
	public ParameterBinder doGetBinder(Column column) {
		ParameterBinder columnBinder = bindersPerColumn.get(column);
		return columnBinder != null ? columnBinder : getBinder(column.getJavaType());
	}
	
	@Override
	public Set<Column> keys() {
		return bindersPerColumn.keySet();
	}
	
	@Override
	public Set<Entry<Column, ParameterBinder>> all() {
		return bindersPerColumn.entrySet();
	}
	
	@Override
	public <T> ResultSetReader<T> doGetReader(Class<T> key) {
		// since ParameterBinder is also a ResultSetReader we just return it
		return getBinder(key);
	}
	
	@Override
	public <T> PreparedStatementWriter<T> doGetWriter(Class<T> key) {
		// since ParameterBinder is also a PreparedStatementWriter we just return it
		return getBinder(key);
	}
}
