package org.gama.stalactite.persistence.engine.model;

import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;

/**
 * @author Guillaume Mary
 */
public class Car extends Vehicle {
	
	private String model;
	
	public Car() {
	}
	
	public Car(Long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Car(Identifier<Long> id) {
		super(id);
	}
	
	public Car(Long id, String model) {
		this(new PersistableIdentifier<>(id), model);
	}
	
	public Car(Identifier<Long> id, String model) {
		super(id);
		setModel(model);
	}
	
	public String getModel() {
		return model;
	}
	
	public void setModel(String model) {
		this.model = model;
	}
	
}
