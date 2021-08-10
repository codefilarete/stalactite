package org.gama.stalactite.persistence.sql;

import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.dml.GeneratedKeysReader;

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
