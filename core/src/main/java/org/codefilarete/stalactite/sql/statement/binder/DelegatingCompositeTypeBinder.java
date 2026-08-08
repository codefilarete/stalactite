package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Function;

/**
 * Implementation of {@link CompositeTypeBinder} with the required and minimum fields to fit the need.
 *
 * @param <C> the mapped type
 * @author Guillaume Mary
 * @see ColumnBinderRegistry
 */
public class DelegatingCompositeTypeBinder<C> implements CompositeTypeBinder<C> {
	
	private final Class<C> boundedType;
	private final Map<Integer, PreparedStatementWriter<?>> componentTypeBinders;
	private final Function<C, Object[]> componentTypeUnsheller;
	private final int componentTypeSize;
	
	/**
	 * Creates a binder for writing composite objects.
	 * Instance should be registered into a {@link ColumnBinderRegistry}
	 * 
	 * @param boundedType the type of the composite object
	 * @param componentTypeBinders a map of binders for each component type of the composite object, where the key is the index of the component and the value is the corresponding binder
	 * @param componentTypeUnsheller a function that takes a composite object and returns an array of its component values
	 */
	public DelegatingCompositeTypeBinder(Class<C> boundedType,
	                                     Map<Integer, PreparedStatementWriter<?>> componentTypeBinders,
	                                     Function<C, Object[]> componentTypeUnsheller) {
		this.boundedType = boundedType;
		this.componentTypeBinders = componentTypeBinders;
		this.componentTypeSize = componentTypeBinders.size();
		this.componentTypeUnsheller = componentTypeUnsheller;
	}
	
	@Override
	public int getComponentTypeSize() {
		return componentTypeSize;	
	}
	
	@Override
	public void set(PreparedStatement preparedStatement, int valueIndex, C value) throws SQLException {
		Object[] unshelledValues = componentTypeUnsheller.apply(value);
		componentTypeBinders.forEach((index, binder) -> {
			try {
				((PreparedStatementWriter<Object>) binder).set(preparedStatement, (valueIndex - 1) * this.componentTypeSize + index + 1, unshelledValues[index]);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
	}
	
	@Override
	public Class<C> getType() {
		return boundedType;
	}
	
	@Override
	public C doGet(ResultSet resultSet, String columnName) {
		// we can't handle the read operation because it goes against this class principle : widespread a composite object over several columns
		throw new UnsupportedOperationException("This invocation is unexpected : this class was made to handle complex type set in a PreparedStatement, not to read them from a ResultSet");
	}
}
