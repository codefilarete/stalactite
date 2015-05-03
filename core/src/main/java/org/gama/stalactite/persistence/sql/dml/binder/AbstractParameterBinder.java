package org.gama.stalactite.persistence.sql.dml.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nonnull;

/**
 * @author mary
 */
public abstract class AbstractParameterBinder<T> implements ParameterBinder<T> {
	
	@Override
	public T get(String columnName, ResultSet resultSet) throws SQLException {
		if (!isNull(columnName, resultSet)) {
			return getNotNull(columnName, resultSet);
		} else {
			return null;
		}
	}
	
	protected boolean isNull(String columnName, ResultSet resultSet) throws SQLException {
		return resultSet.getObject(columnName) == null;
	}

	public abstract T getNotNull(String columnName, ResultSet resultSet) throws SQLException;

	@Override
	public void set(int valueIndex, T value, PreparedStatement statement) throws SQLException {
		if (value == null) {
			setNull(valueIndex, statement);
			// TODO: à implémenter avec un parsing de la requête, cf AbstractQueryImpl#expandParameterList
//		} else if (value instanceof Collection) {
//			Collection listParam = (Collection) value;
//			statement.setParameterList(valueIndex, listParam);
		} else {
			setNotNull(valueIndex, value, statement);
		}
	}

	/**
	 * Cas spécial pour les valeurs null. Implémenté par {@link PreparedStatement#setObject(int, Object)} avec null en
	 * 2nd paramètre, mais tous les Drivers JDBC ne supportent pas cette façon de faire et nécessitent d'utiliser
	 * {@link PreparedStatement#setNull(int, int)}.
	 * A surcharger en fonction du support de setObject(index, null).
	 *
	 * @param valueIndex l'indice auquel il faut mettre null, 1er paramètre de setXXX(..)
	 * @param statement le PreparedStatement sur lequel on veut fixer null
	 * @throws SQLException
	 */
	protected void setNull(int valueIndex, PreparedStatement statement) throws SQLException {
		statement.setObject(valueIndex, null);
	}

	public abstract void setNotNull(int valueIndex, @Nonnull T value, PreparedStatement statement) throws SQLException;
}
