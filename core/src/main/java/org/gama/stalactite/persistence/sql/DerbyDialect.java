package org.gama.stalactite.persistence.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.gama.lang.collection.Iterables;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.dml.GeneratedKeysReader;
import org.gama.sql.dml.WriteOperation;

/**
 * @author Guillaume Mary
 */
public class DerbyDialect extends Dialect {
	
	/**
	 * Overriden to return dedicated Derby generated keys reader because Derby as a special management
	 * <strong>Only supports Integer</strong>
	 */
	@Override
	public <I> GeneratedKeysReader<I> buildGeneratedKeysReader(String keyName, Class<I> columnType) {
		return (GeneratedKeysReader<I>) new DerbyGeneratedKeysReader(keyName);
	}
	
	public static class DerbyGeneratedKeysReader extends GeneratedKeysReader<Integer> {
		
		/**
		 * Constructor
		 *
		 * @param keyName column name to be read on generated {@link ResultSet}
		 */
		public DerbyGeneratedKeysReader(String keyName) {
			super(keyName, DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER);
		}
		
		/** Overriden to simulate generated keys for Derby because it only returns the highest generated key */
		@Override
		public List<Integer> read(WriteOperation writeOperation) throws SQLException {
			List<Integer> rows = super.read(writeOperation);
			// Derby only returns one row: the highest generated key
			int first = Iterables.first(rows);
			// we append the missing values in incrementing order, assuming that's a one by one increment
			for (int i = 0; i < writeOperation.getUpdatedRowCount(); i++) {
				rows.add(0, first - i);
			}
			return rows;
		}
	}
}
