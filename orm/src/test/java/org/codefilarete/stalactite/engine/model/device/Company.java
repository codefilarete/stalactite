package org.codefilarete.stalactite.engine.model.device;

import java.util.HashSet;
import java.util.Set;

import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;

public class Company implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private String name;
	
	private Set<Device> devices;
	
	public Company() {
	}
	
	public Company(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Company(Identifier<Long> id) {
		this.id = id;
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
	
	public Set<Device> getDevices() {
		return this.devices;
	}
	
	public void addDevice(Device device) {
		if (this.devices == null) {
			this.devices = new HashSet<>();
		}
		this.devices.add(device);
	}
}
