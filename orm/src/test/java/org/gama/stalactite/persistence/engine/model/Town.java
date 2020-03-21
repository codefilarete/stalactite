package org.gama.stalactite.persistence.engine.model;

import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;

/**
 * @author Guillaume Mary
 */
public class Town extends City {
	
	private int discotecCount;
	
	public Town() {
	}
	
	public Town(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Town(Identifier<Long> id) {
		super(id);
	}
	
	public int getDiscotecCount() {
		return discotecCount;
	}
	
	public void setDiscotecCount(int discotecCount) {
		this.discotecCount = discotecCount;
	}
}
