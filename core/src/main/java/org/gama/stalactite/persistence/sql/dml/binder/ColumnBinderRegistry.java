package org.gama.stalactite.persistence.sql.dml.binder;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import org.gama.lang.Reflections;
import org.gama.lang.bean.InterfaceIterator;
import org.gama.lang.collection.Iterables;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.binder.ParameterBinderIndex;
import org.gama.sql.binder.ParameterBinderRegistry;
import org.gama.sql.dml.SQLStatement.BindingException;
import org.gama.stalactite.persistence.structure.Column;

/**
 * Registry of {@link ParameterBinder}s per {@link Column} to simplify access to method of {@link PreparedStatement} per {@link Column}.
 * Use {@link #register(Column, ParameterBinder)} or {@link #register(Class, ParameterBinder)} to specify the best
 * binder for a column or type.
 *
 * @author Guillaume Mary
 */
public class ColumnBinderRegistry extends ParameterBinderRegistry implements ParameterBinderIndex<Column, ParameterBinder> {
	
	/**
	 * Registry for Columns
	 */
	private final Map<Column, ParameterBinder> parameterBinders = new HashMap<>();
	
	public ColumnBinderRegistry() {
		// default constructor, properties are already assigned
	}
	
	public <T> void register(Column column, ParameterBinder<T> parameterBinder) {
		this.parameterBinders.put(column, parameterBinder);
	}
	
	/**
	 * Gives the {@link ParameterBinder} of a column if exists, else gives it for the Java type of the {@link Column}
	 *
	 * @param column any non null {@link Column}
	 * @return the binder for the column or for its Java type
	 * @throws BindingException if the binder doesn't exist
	 */
	@Override
	public ParameterBinder doGetBinder(@Nonnull Column column) {
		ParameterBinder columnBinder = parameterBinders.get(column);
		try {
			return columnBinder != null ? columnBinder : getBinder(column.getJavaType());
		} catch (UnsupportedOperationException e) {
			InterfaceIterator interfaceIterator = new InterfaceIterator(column.getJavaType());
			Stream<ParameterBinder> stream = Iterables.stream(interfaceIterator).map(iface -> getParameterBinders().get(iface));
			columnBinder = stream.filter(Objects::nonNull).findFirst().orElse(null);
			if (columnBinder != null) {
				return columnBinder;
			} else {
				// exception is replaced by a better message
				throw newMissingBinderException(column);
			}
		}
	}
	
	@Override
	public Set<Column> keys() {
		return parameterBinders.keySet();
	}
	
	@Override
	public Set<Entry<Column, ParameterBinder>> all() {
		return parameterBinders.entrySet();
	}
	
	private BindingException newMissingBinderException(Column column) {
		return new BindingException("No parameter binder found for column " + column.getAbsoluteName()
				+ " (type " + Reflections.toString(column.getJavaType()) + ")");
	}
}
