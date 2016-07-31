package org.gama.stalactite.persistence.id.generator;

import java.util.Map;

/**
 * @author Guillaume Mary
 */
public interface IdentifierGenerator {
	
	void configure(Map<String, Object> configuration);
}
