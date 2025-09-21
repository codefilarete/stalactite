package org.codefilarete.stalactite.engine.model.device;

import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;

public class Device implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private String name;
	
	private Company manufacturer;
	
	private Location location;
	
	public Device() {
	}
	
	public Device(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Device(Identifier<Long> id) {
		this.id = id;
	}
	
	public Device(long id, String name) {
		this(id);
		setName(name);
	}
	
	public Identifier<Long> getId() {
		return id;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public Company getManufacturer() {
		return manufacturer;
	}
	
	public void setManufacturer(Company manufacturer) {
		this.manufacturer = manufacturer;
	}
	
	public Location getLocation() {
		return location;
	}
	
	public void setLocation(Location location) {
		this.location = location;
	}
}
