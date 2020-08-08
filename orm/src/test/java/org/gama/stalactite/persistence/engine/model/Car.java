package org.gama.stalactite.persistence.engine.model;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;

/**
 * @author Guillaume Mary
 */
public class Car extends Vehicle {
	
	private String model;
	
	private Radio radio;
	
	private Set<String> plates = new HashSet<>();
	
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
	
	public Radio getRadio() {
		return radio;
	}
	
	public void setRadio(Radio radio) {
		this.radio = radio;
		radio.setCar(this);
	}
	
	public Set<String> getPlates() {
		return plates;
	}
	
	public void addPlate(String plateNumber) {
		this.plates.add(plateNumber);
	}
	
	public static abstract class AbstractRadio {
		private Car car;
		
		public Car getCar() {
			return car;
		}
		
		public void setCar(Car car) {
			this.car = car;
		}
		
		
	}
	
	public static class Radio extends AbstractRadio {
		
		private String serialNumber;
		
		private String model;
		
		private boolean persisted;
		
		private Radio() {
		}
		
		public Radio(String serialNumber) {
			this.serialNumber = serialNumber;
		}
		
		public String getSerialNumber() {
			return serialNumber;
		}
		
		public String getModel() {
			return model;
		}
		
		public Radio setModel(String model) {
			this.model = model;
			return this;
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
