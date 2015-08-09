package org.gama.stalactite.persistence.mapping;

import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Map;

/**
 * @author mary
 */
public interface IMappingStrategy<T> {
	
	Map<Column, Object> getInsertValues(T t);
	
	Map<UpwhereColumn, Object> getUpdateValues(T modified, T unmodified, boolean allColumns);
	
	Map<Column, Object> getDeleteValues(@Nonnull T t);
	
	Map<Column, Object> getSelectValues(@Nonnull Serializable id);
	
	Map<Column, Object> getVersionedKeyValues(@Nonnull T t);
	
	Table getTargetTable();
	
	Serializable getId(T t);
	
	void setId(T t, Serializable identifier);
	
	T transform(Row row);
}
