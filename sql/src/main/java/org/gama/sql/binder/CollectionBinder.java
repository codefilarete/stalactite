package org.gama.sql.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Guillaume Mary
 */
public class CollectionBinder<T extends Iterable<E>, E> implements ParameterBinder<T> {
	
	private final ParameterBinder<E> delegateBinder;
	
	public CollectionBinder(ParameterBinder<E> delegateBinder) {
		this.delegateBinder = delegateBinder;
	}
	
	@Override
	public T get(String columnName, ResultSet resultSet) throws SQLException {
		throw new UnsupportedOperationException(CollectionBinder.class.getName() + " is not aimed at getting values from ResultSet");
	}
	
	@Override
	public void set(int valueIndex, T value, PreparedStatement statement) throws SQLException {
		int i = valueIndex;
		for (E e : value) {
			delegateBinder.set(i++, e, statement);
		}
	}
}
