package org.gama.stalactite.persistence.id.generator;

import java.util.Map;

/**
 * @author Guillaume Mary
 */
public interface IdAssignmentPolicy {
	
	void configure(Map<String, Object> configuration);
}
