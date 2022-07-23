package org.codefilarete.stalactite.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

import org.codefilarete.tool.exception.NotImplementedException;

/**
 * Basic contract to complete a bean from a {@link ResultSet} row.
 * 
 * @author Guillaume Mary
 */
public interface ResultSetRowAssembler<T> extends CopiableForAnotherQuery<T> {
	
	/**
	 * Expected to read columns from given {@link ResultSet} which is positioned on a row (not before first read, neither after last) 
	 *
	 * @param rootBean the bean built for the row or a set of row
	 * @param resultSet the one to read, call to next() is highly unexpected unless you're sure of your data and you know caller iteration algorithm 
	 * @throws SQLException this returned by {@link ResultSet} read
	 */
	void assemble(T rootBean, ResultSet resultSet) throws SQLException;
	
	/**
	 * Overridden for return type cast.
	 */
	default ResultSetRowAssembler<T> copyWithAliases(Function<String, String> columnMapping) {
		throw new NotImplementedException("This instance doesn't support copy, please implement it if you wish to reuse its mapping for another query");
	}
}
