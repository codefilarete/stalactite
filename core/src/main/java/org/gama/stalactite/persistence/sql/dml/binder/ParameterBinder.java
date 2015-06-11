package org.gama.stalactite.persistence.sql.dml.binder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Guillaume Mary
 */
public interface ParameterBinder<T> {
	
	/**
	 * Read column <t>columnName</t> returned by <t>resultSet</t>
	 * 
	 * @param columnName
	 * @param resultSet
	 * @return content of <t>columnName</t>, typed according to <t>column</t>
	 * @throws SQLException
	 */
	T get(String columnName, ResultSet resultSet) throws SQLException;

	/**
	 * Applies <t>value</t> at position <t>valueIndex</t> on <t>statement</t>.
	 * 
	 * @param valueIndex parameter index to be set, value for first parameter of methods <t>Statement.setXXX(..)</t>
	 * @param value value to be passed as second argument of methods <t>Statement.setXXX(..)</t>
	 * @param statement PreparedStatement to be used
	 * @throws SQLException
	 */
	void set(int valueIndex, T value, PreparedStatement statement) throws SQLException;
}
