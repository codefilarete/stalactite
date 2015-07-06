package org.gama.stalactite.persistence.mapping;

import org.gama.stalactite.persistence.sql.result.Row;
import org.gama.stalactite.persistence.structure.Table.Column;

import java.util.Map;
import java.util.Set;

/**
 * Mapper for embedded beans in another. For instance a simple @OneToOne can be considered as an embedded bean.
 * 
 * @author Guillaume Mary
 */
public interface IEmbeddedBeanMapper<T> {
	
	Map<Column, Object> getInsertValues(T t);
	
	Map<Column, Object> getUpdateValues(T modified, T unmodified, boolean allColumns);
	
	Set<Column> getColumns();
	
	T transform(Row row);
}
