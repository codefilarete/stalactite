package org.gama.stalactite.persistence.sql.dml.binder;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.ParameterBinderRegistry;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

import java.sql.PreparedStatement;
import java.util.HashMap;

/**
 * Registry of {@link ParameterBinder}s used pickup the best suited Column to simplify access to method {@link
 * PreparedStatement}.
 * Use {@link #register(Column, ParameterBinder)} or {@link #register(Class, ParameterBinder)} to specify the best
 * binder for a column or type.
 *
 * @author Guillaume Mary
 */
public class ColumnBinderRegistry {
	
	/**
	 * Registry for types
	 */
	private ParameterBinderRegistry parameterBinderRegistry = new ParameterBinderRegistry();
	
	/**
	 * Registry for Columns
	 */
	private final HashMap<String /* column absolute name */, ParameterBinder> parameterBinders = new HashMap<>();
	
	public ColumnBinderRegistry() {
	}
	
	public <T> void register(Column column, ParameterBinder<T> parameterBinder) {
		this.parameterBinders.put(column.getAbsoluteName(), parameterBinder);
	}
	
	public <T> void register(Class<T> clazz, ParameterBinder<T> parameterBinder) {
		this.parameterBinderRegistry.register(clazz, parameterBinder);
	}
	
	/**
	 * Gives the {@link ParameterBinder} of a column is exists, else gives it for the Java type of the Column
	 *
	 * @param column
	 * @return the binder for the column or for its Java type
	 * @throws UnsupportedOperationException if the binder doesn't exist
	 */
	public ParameterBinder getBinder(Table.Column column) {
		ParameterBinder columnBinder = parameterBinders.get(column.getAbsoluteName());
		try {
			return columnBinder != null ? columnBinder : getBinder(column.getJavaType());
		} catch (UnsupportedOperationException e) {
			// basic exception is replaced by a better message
			throwMissingBinderException(column);
			return null;	// unreachable
		}
	}
	
	/**
	 * Gives the registered {@link ParameterBinder} for the given type.
	 *
	 * @param clazz a class
	 * @return the registered {@link ParameterBinder} for the given type
	 * @throws UnsupportedOperationException if the binder doesn't exist
	 */
	public ParameterBinder getBinder(Class clazz) {
		return this.parameterBinderRegistry.getBinder(clazz);
	}
	
	private void throwMissingBinderException(Table.Column column) {
		throw new UnsupportedOperationException("No parameter binder found for column " + column.getAbsoluteName() + " (type " + column.getJavaType() + ")");
	}
}
