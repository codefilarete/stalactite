package org.codefilarete.stalactite.query.model;

import java.util.Set;

/**
 * Contract for elements that can provide {@link org.codefilarete.stalactite.sql.ddl.structure.Column}s, and in a wider way, {@link Selectable}s,
 * considered items from a Select clause of an SQL query.
 * 
 * @author Guillaume Mary
 */
public interface SelectablesPod {
	
	/**
	 * Returns a read-only view of selected items
	 * 
	 * @return non modifiable
	 */
	Set<Selectable> getColumns();
}
