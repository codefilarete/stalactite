package org.gama.stalactite.persistence.id;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Guillaume Mary
 */
public interface IdentifierGenerator {
	
	Serializable generate();
	
	void configure(Map<String, Object> configuration);
}
