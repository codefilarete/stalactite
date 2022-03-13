package org.codefilarete.stalactite.engine.model;

import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.Identifier;

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
