package org.gama.stalactite.benchmark;

import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;

/**
 * @author Guillaume Mary
 */
public interface IMappingBuilder {
	ClassMappingStrategy getClassMappingStrategy();
}
