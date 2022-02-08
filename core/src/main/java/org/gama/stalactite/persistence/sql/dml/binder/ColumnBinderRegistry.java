package org.gama.stalactite.persistence.sql.dml.binder;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.bean.InterfaceIterator;
import org.codefilarete.tool.collection.Iterables;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.gama.stalactite.sql.binder.ParameterBinderIndex;
import org.gama.stalactite.sql.binder.ParameterBinderRegistry;
import org.gama.stalactite.sql.dml.SQLStatement.BindingException;
import org.gama.stalactite.persistence.structure.Column;

/**
 * Registry of {@link ParameterBinder}s per {@link Column} to simplify access to method of {@link PreparedStatement} for {@link Column}s.
 * See {@link #register(Column, ParameterBinder)} and {@link #register(Class, ParameterBinder)} to specify binder for a column or type.
 *
 * @author Guillaume Mary
 */
public class ColumnBinderRegistry extends ParameterBinderRegistry implements ParameterBinderIndex<Column, ParameterBinder> {
	
	/**
	 * Registry for {@link Column}s
	 */
	private final Map<Column, ParameterBinder> bindersPerColumn = new HashMap<>();
	
	public ColumnBinderRegistry() {
		// default constructor, properties are already assigned
	}
	
	public <T> void register(Column column, ParameterBinder<T> parameterBinder) {
		ParameterBinder existingBinder = this.bindersPerColumn.get(column);
		if (existingBinder != null && !existingBinder.equals(parameterBinder)) { 
			throw new BindingException("Binder for column " + column + " already exists");
		}
		this.bindersPerColumn.put(column, parameterBinder);
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
		ParameterBinder columnBinder = bindersPerColumn.get(column);
		try {
			return columnBinder != null ? columnBinder : getBinder(column.getJavaType());
		} catch (UnsupportedOperationException e) {
			InterfaceIterator interfaceIterator = new InterfaceIterator(column.getJavaType());
			Stream<ParameterBinder> stream = Iterables.stream(interfaceIterator).map(this::getBinder);
			columnBinder = stream.filter(Objects::nonNull).findFirst().orElse(null);
			if (columnBinder != null) {
				return columnBinder;
			} else {
				// Why do we throw an exception instead of returning null ? because null would generate a NullPointerException for caller
				// which should handle this case with ... what ? in fact there's no solution to missing binder else than throwing an exception
				// saying that configuration is insufficient, that's what we do.
				throw new BindingException("No binder found for column " + column.getAbsoluteName()
						+ " (type " + Reflections.toString(column.getJavaType()) + ")");
			}
		}
	}
	
	@Override
	public Set<Column> keys() {
		return bindersPerColumn.keySet();
	}
	
	@Override
	public Set<Entry<Column, ParameterBinder>> all() {
		return bindersPerColumn.entrySet();
	}
	
}
