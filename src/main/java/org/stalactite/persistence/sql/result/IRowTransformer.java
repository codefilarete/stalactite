package org.stalactite.persistence.sql.result;

import java.sql.SQLException;

/**
 * @author mary
 */
public interface IRowTransformer<T> {
	
	public T transform(Row row) throws SQLException;
}
