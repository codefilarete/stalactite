package org.codefilarete.stalactite.query.model;

import java.util.Map;

import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * The interface defining what's possible to do (fluent point of view) on a select
 * 
 * @author Guillaume Mary
 */
public interface SelectChain<T extends SelectChain<T>> {
	
	T add(Object selectable, Object... selectables);
	
	T add(Column column, String alias);
	
	default T add(Column col1, String alias1, Column col2, String alias2) {
		return add(col1, alias1).add(col2, alias2);
	}
	
	default T add(Column col1, String alias1, Column col2, String alias2, Column col3, String alias3) {
		return add(col1, alias1).add(col2, alias2).add(col3, alias3);
	}
	
	T add(Map<Column, String> aliasedColumns);
	
	T distinct();
}
