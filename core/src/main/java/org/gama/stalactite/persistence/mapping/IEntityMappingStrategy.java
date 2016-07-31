package org.gama.stalactite.persistence.mapping;

import java.io.Serializable;

/**
 * @author mary
 */
public interface IEntityMappingStrategy<T> extends IMappingStrategy<T> {
	
	Serializable getId(T t);
	
	void setId(T t, Serializable identifier);
}
