package org.gama.stalactite.persistence.engine.model;

import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;

/**
 * @author Guillaume Mary
 */
public class Vehicle extends AbstractVehicle {
	
	private Color color;
	
	private Engine engine;
	
	private Person owner;
	
	public Vehicle(Long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Vehicle(Identifier<Long> id) {
		super(id);
	}
	
	public Vehicle() {
	}
	
	public Color getColor() {
		return color;
	}
	
	public void setColor(Color color) {
		this.color = color;
	}
	
	public Engine getEngine() {
		return engine;
	}
	
	public void setEngine(Engine engine) {
		this.engine = engine;
	}
	
	public Person getOwner() {
		return owner;
	}
	
	public void setOwner(Person owner) {
		this.owner = owner;
	}
}
