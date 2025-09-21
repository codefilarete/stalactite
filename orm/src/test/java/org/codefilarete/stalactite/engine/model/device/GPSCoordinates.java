package org.codefilarete.stalactite.engine.model.device;

import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;

public class GPSCoordinates extends Location {
	
	private double latitude;
	private double longitude;
	
	public GPSCoordinates() {
	}
	
	public GPSCoordinates(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public GPSCoordinates(Identifier<Long> id) {
		super(id);
	}
	
	public double getLatitude() {
		return latitude;
	}
	
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	
	public double getLongitude() {
		return longitude;
	}
	
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
}
