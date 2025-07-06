package org.codefilarete.stalactite.engine.model;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;

/**
 * @author Guillaume Mary
 */
public class Engine implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private double displacement;
	
	private Vehicle vehicle;
	
	public Engine() {
	}
	
	public Engine(Long id) {
		this.id = new PersistableIdentifier<>(id);
	}
	
	public Engine(Identifier<Long> id) {
		this.id = id;
	}
	
	public Identifier<Long> getId() {
		return id;
	}
	
	public double getDisplacement() {
		return displacement;
	}
	
	public void setDisplacement(double displacement) {
		this.displacement = displacement;
	}
	
	public Vehicle getVehicle() {
		return vehicle;
	}
	
	public void setVehicle(Vehicle vehicle) {
		this.vehicle = vehicle;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		
		Engine engine = (Engine) o;
		
		return Objects.equals(id, engine.id);
	}
	
	@Override
	public int hashCode() {
		return id.hashCode();
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
