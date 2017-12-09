package org.gama.stalactite.persistence.mapping;

import java.util.Map;

import org.gama.sql.result.Row;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Column;

/**
 * A very general contract for mapping a type to a database table. Not expected to be used as this (for instance it lacks deletion contract)
 * 
 * @author Guillaume Mary
 */
public interface IMappingStrategy<T> {
	
	Map<Column, Object> getInsertValues(T t);
	
	Map<UpwhereColumn, Object> getUpdateValues(T modified, T unmodified, boolean allColumns);
	
	T transform(Row row);
	
}
