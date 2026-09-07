package org.codefilarete.stalactite.engine.model;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractVehicle implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private Timestamp timestamp;
	
	private Person owner;
	
	public AbstractVehicle() {
	}
	
	protected AbstractVehicle(Identifier<Long> id) {
		this.id = id;
	}
	
	@Override
	public Identifier<Long> getId() {
		return id;
	}
	
	public Timestamp getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}
	
	public Person getOwner() {
		return owner;
	}
	
	public void setOwner(Person owner) {
		this.owner = owner;
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
