package org.codefilarete.stalactite.sql.ddl;

import java.util.List;

/**
 * A simple contract for classes that want to participate to {@link DDLGenerator}
 * 
 * @author Guillaume Mary
 */
public interface DDLProvider {
	
	List<String> getCreationScripts();
	
	List<String> getDropScripts();
}
