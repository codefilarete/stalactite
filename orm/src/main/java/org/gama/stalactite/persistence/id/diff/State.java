package org.gama.stalactite.persistence.id.diff;

/**
 * @author Guillaume Mary
 */
public enum State {
	/** The object has been added to the collection */
	ADDED,
	/**
	 * The object exists in both Sets, but source and replacer may differ on some fields
	 * (depending on {@link #equals(Object)} method exhaustivity)
	 */
	HELD,
	/** The object has been removed from the source object */
	REMOVED
}
