package org.stalactite.persistence.mapping;

import java.io.Serializable;
import java.util.Set;

import javax.annotation.Nonnull;

import org.stalactite.persistence.sql.result.Row;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public interface IMappingStrategy<T> {
	
	PersistentValues getInsertValues(T t);
	
	PersistentValues getUpdateValues(T modified, T unmodified, boolean allColumns);
	
	PersistentValues getDeleteValues(@Nonnull T t);
	
	PersistentValues getSelectValues(@Nonnull Serializable id);
	
	PersistentValues getVersionedKeyValues(@Nonnull T t);
	
	Table getTargetTable();
	
	Serializable getId(T t);
	
	Set<Column> getColumns();
	
	public T transform(Row row);
}
