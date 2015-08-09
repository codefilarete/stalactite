package org.gama.stalactite.persistence.sql.ddl;

import java.util.List;

/**
 * @author Guillaume Mary
 */
public interface DDLGenerator {
	
	List<String> getCreationScripts();
	
	List<String> getDropScripts();
}
