package org.gama.sql.dml;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.gama.lang.reflect.MemberPrinter;
import org.gama.sql.ConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SQLOperation} dedicated to Selectes ... so all operations that return a ResultSet
 *
 * @author Guillaume Mary
 */
public class ReadOperation<ParamType> extends SQLOperation<ParamType> {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SQLOperation.class);
	private static final Logger VALUES_LOGGER = LoggerFactory.getLogger(MemberPrinter.FULL_PACKAGE_PRINTER.toString(SQLOperation.class) + ".values");
	
	public ReadOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider) {
		super(sqlGenerator, connectionProvider);
	}
	
	/**
	 * Executes the statement, wraps {@link PreparedStatement#executeQuery()}
	 *
	 * @return the {@link ResultSet} from the database
	 */
	public ResultSet execute() {
		try {
			ensureStatement();
			this.sqlStatement.applyValues(preparedStatement);
			LOGGER.debug(getSQL());
			VALUES_LOGGER.debug("{}", sqlStatement.getValues());
			return this.preparedStatement.executeQuery();
		} catch (SQLException e) {
			throw new RuntimeException("Error running \"" + getSQL() + "\"", e);
		}
	}
}
