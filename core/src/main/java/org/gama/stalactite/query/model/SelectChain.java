package org.gama.stalactite.query.model;

import java.util.Map;

import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * The interface defining what's possible to do (fluent point of view) on a select
 * 
 * @author Guillaume Mary
 */
public interface SelectChain<T extends SelectChain<T>> {
	
	T add(Column column);
	
	T add(Column ... columns);
	
	T add(String ... columns);
	
	T add(Column column, String alias);
	
	T add(Map<Column, String> aliasedColumns);
}
