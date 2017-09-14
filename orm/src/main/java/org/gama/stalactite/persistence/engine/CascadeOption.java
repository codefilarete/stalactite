package org.gama.stalactite.persistence.engine;

/**
 * @author Guillaume Mary
 */
public interface CascadeOption<R> {
	
	R cascade(CascadeType cascadeType, CascadeType ... cascadeTypes);
	
	enum CascadeType {
		INSERT,
		UPDATE,
		DELETE,
		/** For fetching relations. Without it, some {@link NullPointerException} may occur if you didn't fulfill the relations by yourself */
		SELECT
	}
}
