package org.codefilarete.stalactite.engine.model.device;

import org.codefilarete.stalactite.id.Identified;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;

import java.time.LocalDateTime;

public class Review implements Identified<Long> {
	
	private Identifier<Long> id;
	
	private int ranking;
	
	private LocalDateTime date;
	
	private Location location;
	
	public Review() {
	}
	
	public Review(long id) {
		this(new PersistableIdentifier<>(id));
	}
	
	public Review(Identifier<Long> id) {
		this.id = id;
	}
	
	public Identifier<Long> getId() {
		return id;
	}
	
	public int getRanking() {
		return ranking;
	}
	
	public void setRanking(int ranking) {
		this.ranking = ranking;
	}
	
	public LocalDateTime getDate() {
		return date;
	}
	
	public void setDate(LocalDateTime date) {
		this.date = date;
	}
	
	public Location getLocation() {
		return location;
	}
	
	public void setLocation(Location location) {
		this.location = location;
	}
}
