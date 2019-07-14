package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.structure.Table;

/**
 * Contract for inheritance options
 * 
 * @author Guillaume Mary
 */
public interface InheritanceOptions {
	
	InheritanceOptions withJoinedTable();
	
	InheritanceOptions withJoinedTable(Table parentTable);
	
}