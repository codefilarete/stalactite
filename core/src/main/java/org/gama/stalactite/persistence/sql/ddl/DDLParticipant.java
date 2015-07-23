package org.gama.stalactite.persistence.sql.ddl;

import java.util.List;

/**
 * @author Guillaume Mary
 */
public interface DDLParticipant {
	
	List<String> getCreationScripts();
	
	List<String> getDropScripts();
}
