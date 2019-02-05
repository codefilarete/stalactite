package org.gama.stalactite.persistence.engine.model;

import java.util.Date;

/**
 * @author Guillaume Mary
 */
public class Timestamp {
	
	private Date creationDate;
	
	private Date modificationDate;
	
	public Timestamp() {
		this.creationDate = new Date();
		this.modificationDate = new Date();
	}
	
	public Date getCreationDate() {
		return creationDate;
	}
	
	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}
	
	public Date getModificationDate() {
		return modificationDate;
	}
	
	public void setModificationDate(Date modificationDate) {
		this.modificationDate = modificationDate;
	}
}
