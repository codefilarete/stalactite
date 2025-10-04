package org.codefilarete.stalactite.dsl;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Contract for inheritance options
 * 
 * @author Guillaume Mary
 */
public interface InheritanceOptions {
	
	InheritanceOptions withJoinedTable();
	
	InheritanceOptions withJoinedTable(Table parentTable);
	
}