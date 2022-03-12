package org.codefilarete.stalactite.persistence.sql;

import java.sql.SQLException;
import java.util.List;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.stalactite.persistence.sql.DerbyDialect.DerbyWriteOperation;
import org.codefilarete.stalactite.sql.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.dml.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.dml.WriteOperation;

/**
 * @author Guillaume Mary
 */
public class DerbyGeneratedKeysReader extends GeneratedKeysReader<Integer> {
	
	/**
	 * Constructor
	 */
	public DerbyGeneratedKeysReader() {
		// Derby doesn't support named column for generated key, we use "1" assuming this is used for autoincremented id
		super("1", DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER);
	}
	
	/** Overriden to simulate generated keys for Derby because it only returns the highest generated key */
	@Override
	public List<Integer> read(WriteOperation writeOperation) throws SQLException {
		List<Integer> rows = super.read(writeOperation);
		// Derby only returns one row: the highest generated key
		int first = Iterables.first(rows);
		// we append the missing values in incrementing order, assuming that's a one by one increment
		for (int i = 0; i < ((DerbyWriteOperation) writeOperation).getUpdatedRowCount(); i++) {
			rows.add(0, first - i);
		}
		return rows;
	}
}
