package org.codefilarete.stalactite.engine.model;

/**
 * @author Guillaume Mary
 */
public class Bicycle extends AbstractVehicle {

	private Color color;

	private Person owner;

	public Color getColor() {
		return color;
	}

	public void setColor(Color color) {
		this.color = color;
	}

	public Person getOwner() {
		return owner;
	}

	public void setOwner(Person owner) {
		this.owner = owner;
	}
}
