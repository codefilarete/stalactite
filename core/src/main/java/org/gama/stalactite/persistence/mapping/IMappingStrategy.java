package org.gama.stalactite.persistence.mapping;

import java.io.Serializable;
import java.util.Map;

import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public interface IMappingStrategy<T> {
	
	Map<Column, Object> getInsertValues(T t);
	
	Map<UpwhereColumn, Object> getUpdateValues(T modified, T unmodified, boolean allColumns);
	
	Map<Column, Object> getDeleteValues(T t);
	
	Map<Column, Object> getSelectValues(Serializable id);
	
	Map<Column, Object> getVersionedKeyValues(T t);
	
	Table getTargetTable();
	
	Serializable getId(T t);
	
	void setId(T t, Serializable identifier);
	
	T transform(Row row);
}
