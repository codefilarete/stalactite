package org.gama.stalactite.persistence.engine.model;

import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;

/**
 * @author Guillaume Mary
 */
public class Village extends City {
	
	private int barCount;
	
	public Village() {
	}
	
	public Village(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Village(Identifier<Long> id) {
		super(id);
	}
	
	public int getBarCount() {
		return barCount;
	}
	
	public void setBarCount(int barCount) {
		this.barCount = barCount;
	}
}
