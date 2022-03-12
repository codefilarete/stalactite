package org.codefilarete.stalactite.persistence.engine.runtime;

import java.util.Set;

/**
 * @author Guillaume Mary
 */
public interface PolymorphicPersister<C> {
	
	Set<Class<? extends C>> getSupportedEntityTypes();
}
