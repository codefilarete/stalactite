package org.stalactite.benchmark;

import org.stalactite.persistence.mapping.ClassMappingStrategy;

/**
 * @author Guillaume Mary
 */
public interface IMappingBuilder {
	ClassMappingStrategy getClassMappingStrategy();
}
