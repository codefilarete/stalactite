package org.codefilarete.stalactite.persistence.engine;

import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;

/**
 * Contract for inheritance options
 * 
 * @author Guillaume Mary
 */
public interface InheritanceOptions {
	
	InheritanceOptions withJoinedTable();
	
	InheritanceOptions withJoinedTable(Table parentTable);
	
}