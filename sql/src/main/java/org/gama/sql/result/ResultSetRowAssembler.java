package org.gama.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Basic contract to assemble
 * 
 * @author Guillaume Mary
 */
public interface ResultSetRowAssembler<T> {
	
	/**
	 * Expected to read columns from given {@link ResultSet} which is positioned on a row (not before first read, neither after last) 
	 *
	 * @param rootBean the bean built for the row or a set of row
	 * @param resultSet the one to read, call to next() is highly unexpected unless you're sure of your data and you know caller iteration algorithm 
	 * @throws SQLException this returned by {@link ResultSet} read
	 */
	void assemble(T rootBean, ResultSet resultSet) throws SQLException;
}
