package org.gama.stalactite.persistence.id;

import java.util.Map;

/**
 * @author Guillaume Mary
 */
public interface IdentifierGenerator {
	
	void configure(Map<String, Object> configuration);
}
