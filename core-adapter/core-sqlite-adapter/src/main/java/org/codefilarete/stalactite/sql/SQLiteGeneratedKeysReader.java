package org.codefilarete.stalactite.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;

import org.codefilarete.stalactite.sql.SQLiteDialect.SQLiteWriteOperation;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.tool.collection.Iterables;

/**
 * @author Guillaume Mary
 */
public class SQLiteGeneratedKeysReader extends GeneratedKeysReader<Integer> {
	
	public static final String GENERATED_KEYS_COLUMN_NAME = "last_id";
	
	/**
	 * Constructor
	 */
	public SQLiteGeneratedKeysReader() {
		// SQLite doesn't support named column for generated key, we use "1" assuming this is used for autoincremented id
		super(GENERATED_KEYS_COLUMN_NAME, DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER);
	}
	
	/** Overridden to simulate generated keys for SQLite because it only returns the highest generated key */
	@Override
	public List<Integer> convert(WriteOperation writeOperation) {
		// SQLite uses a function to retrieve the last inserted identifier, we have to call it and then deduce the generated values
		ReadOperation<Integer> lastInsertRowIdReader = new ReadOperation<>(new SQLStatement<Integer>(Collections.emptyMap()) {
			@Override
			public String getSQL() {
				return "select last_insert_rowid() as " + GENERATED_KEYS_COLUMN_NAME;
			}
			
			@Override
			protected void doApplyValue(Integer key, Object value, PreparedStatement statement) {
				
			}
		}, writeOperation.getConnectionProvider());
		
		try (ReadOperation<Integer> readOperation = lastInsertRowIdReader) {
			ResultSet execute = readOperation.execute();
			List<Integer> rows = super.convert(execute);
			
			// SQLite only returns one row: the highest generated key
			int first = Iterables.first(rows);
			// we append the missing values in incrementing order, assuming that's a one by one increment
			for (int i = 0; i < ((SQLiteWriteOperation) writeOperation).getUpdatedRowCount(); i++) {
				rows.add(0, first - i);
			}
			return rows;
		}
	}
}
