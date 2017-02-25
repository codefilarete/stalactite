package org.gama.sql.binder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * An interface that allows to use {@link PreparedStatement#setXXX(...)} methods to be used as method reference.
 * 
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface PreparedStatementWriter<I> {
	
	/**
	 * Applies <t>value</t> at position <t>valueIndex</t> on <t>statement</t>.
	 *
	 * @param preparedStatement PreparedStatement to be used
	 * @param valueIndex parameter index to be set, value for first parameter of methods <t>Statement.setXXX(..)</t>
	 * @param value value to be passed as second argument of methods <t>Statement.setXXX(..)</t>
	 * @throws SQLException the exception thrown be the underlying access to the {@link PreparedStatement}
	 */
	void set(PreparedStatement preparedStatement, int valueIndex, I value) throws SQLException;
}
