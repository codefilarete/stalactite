package org.gama.stalactite.persistence.sql.ddl;

import java.util.List;

/**
 * @author mary
 */
public interface DDLParticipant {
	
	List<String> getCreationScripts();
	
	List<String> getDropScripts();
}
