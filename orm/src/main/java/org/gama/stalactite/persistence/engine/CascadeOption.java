package org.gama.stalactite.persistence.engine;

/**
 * @author Guillaume Mary
 */
public interface CascadeOption<R> {
	
	R cascade(CascadeType cascadeType, CascadeType ... cascadeTypes);
	
	enum CascadeType {
		INSERT,
		UPDATE,
		DELETE
	}
}
