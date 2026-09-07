package org.codefilarete.stalactite.engine.model;

import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;

/**
 * @author Guillaume Mary
 */
public class Bicycle extends AbstractVehicle {

	private Color color;
	
	public Bicycle() {
	}
	
	public Bicycle(Long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Bicycle(Identifier<Long> id) {
		super(id);
	}

	public Color getColor() {
		return color;
	}

	public void setColor(Color color) {
		this.color = color;
	}
}
