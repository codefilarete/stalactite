package org.gama.stalactite.persistence.mapping;

import org.gama.stalactite.sql.result.Row;

/**
 * Interface for classes capable of transforming a ResultSet row (represented by {@link Row} into any "more Object" instance.
 * 
 * @author Guillaume Mary
 */
public interface IRowTransformer<T> {
	
	T transform(Row row);
}
