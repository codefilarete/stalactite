package org.codefilarete.stalactite.engine.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.Identifier;

/**
 * @author Guillaume Mary
 */
public class Vehicle extends AbstractVehicle {
	
	private Color color;
	
	private Engine engine;
	
	private Person owner;
	
	private List<Wheel> wheels = new ArrayList<>();
	
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
	
	public List<Wheel> getWheels() {
		return wheels;
	}
	
	public void setWheels(List<Wheel> wheels) {
		this.wheels = wheels;
	}
	
	public void addWheel(Wheel wheel) {
		this.wheels.add(wheel);
		wheel.setVehicle(this);
	}
	
	public static class Wheel {
		
		private String serialNumber;
		
		private String model;
		
		private Vehicle vehicle;
		
		private boolean persisted;
		
		private Wheel() {
		}
		
		public Wheel(String serialNumber) {
			this.serialNumber = serialNumber;
		}
		
		public String getSerialNumber() {
			return serialNumber;
		}
		
		public String getModel() {
			return model;
		}
		
		public Wheel setModel(String model) {
			this.model = model;
			return this;
		}
		
		public Vehicle getVehicle() {
			return vehicle;
		}
		
		public void setVehicle(Vehicle vehicle) {
			this.vehicle = vehicle;
		}
		
		public boolean isPersisted() {
			return persisted;
		}
		
		public void markAsPersisted() {
			this.persisted = true;
		}
		
		@Override
		public boolean equals(Object o) {
			return EqualsBuilder.reflectionEquals(this, o);
		}
		
		@Override
		public int hashCode() {
			return HashCodeBuilder.reflectionHashCode(this);
		}
		
		/**
		 * Implemented for easier debug
		 *
		 * @return a simple representation of this
		 */
		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}
	}
}
