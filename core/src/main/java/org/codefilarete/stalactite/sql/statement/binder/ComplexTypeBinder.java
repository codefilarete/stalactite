package org.codefilarete.stalactite.sql.statement.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.tool.function.ThrowingConverter;

/**
 * Frame for handling unexpected type (from the library) that needs to be persisted into a single column.
 * Acts more as a how-to or proof-of-concept than a really value-added class because there's quite no magic here : it only composes a binder and 2
 * converters for reading and writing to a column. Moreover it can be done directly by creating your own {@link ParameterBinder} and implementing
 * its methods.
 *
 * @param <C> the mapped type
 * @author Guillaume Mary
 * @see SqlTypeRegistry
 * @see ColumnBinderRegistry
 */
public class ComplexTypeBinder<C> implements ParameterBinder<C> {
	
	private final NullAwareParameterBinder<C> convertingBinder;
	
	/**
	 * Creates a binder for persisting C objects (handling eventually null values).
	 * Instance should be registered into a {@link SqlTypeRegistry} and {@link
	 * ColumnBinderRegistry}
	 * 
	 * @param lowerBinder the binder that interacts with {@link PreparedStatement} and {@link ResultSet}
	 * @param toObjectConverter a converter applied on read value given by lower binder when reading from a {@link ResultSet}
	 * @param toDatabaseConverter a converter applied on input value, then result is passed to lower binder when writing to {@link PreparedStatement}
	 * @param <P> the intermediary type
	 */
	public <P> ComplexTypeBinder(ParameterBinder<P> lowerBinder,
								 ThrowingConverter<P, C, RuntimeException> toObjectConverter,
								 ThrowingConverter<C, P, RuntimeException> toDatabaseConverter) {
		convertingBinder = new NullAwareParameterBinder<>(new LambdaParameterBinder<>(
				// we simply need some conversion after reading and before writing
				lowerBinder.thenApply(toObjectConverter::convert), lowerBinder.preApply(toDatabaseConverter::convert)
		));
	}
	
	@Override
	public void set(PreparedStatement preparedStatement, int valueIndex, C value) throws SQLException {
		convertingBinder.set(preparedStatement, valueIndex, value);
	}
	
	@Override
	public Class<C> getType() {
		return convertingBinder.getType();
	}
	
	@Override
	public C doGet(ResultSet resultSet, String columnName) {
		return convertingBinder.get(resultSet, columnName);
	}
}
