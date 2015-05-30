package org.gama.stalactite.persistence.mapping;

import java.io.Serializable;
import java.util.Set;

import javax.annotation.Nonnull;

import org.gama.stalactite.persistence.sql.result.Row;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public interface IMappingStrategy<T> {
	
	StatementValues getInsertValues(T t);
	
	StatementValues getUpdateValues(T modified, T unmodified, boolean allColumns);
	
	StatementValues getDeleteValues(@Nonnull T t);
	
	StatementValues getSelectValues(@Nonnull Serializable id);
	
	StatementValues getVersionedKeyValues(@Nonnull T t);
	
	Table getTargetTable();
	
	Serializable getId(T t);
	
	void setId(T t, Serializable identifier);
	
	Set<Column> getColumns();
	
	public T transform(Row row);
}
