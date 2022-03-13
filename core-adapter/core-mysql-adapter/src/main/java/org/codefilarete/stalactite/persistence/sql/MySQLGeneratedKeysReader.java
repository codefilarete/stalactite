package org.codefilarete.stalactite.persistence.sql;

import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;

/**
 * @author Guillaume Mary
 */
public class MySQLGeneratedKeysReader extends GeneratedKeysReader<Integer> {
	
	/**
	 * Constructor
	 */
	public MySQLGeneratedKeysReader() {
		// For MySQL generated key column is always named "GENERATED_KEY", could also be retrieved through column number 1
		super("GENERATED_KEY", DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER);
	}
}
