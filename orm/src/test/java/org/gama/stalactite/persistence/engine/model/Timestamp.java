package org.gama.stalactite.persistence.engine.model;

import java.util.Date;

/**
 * @author Guillaume Mary
 */
public class Timestamp {
	
	private Date creationDate;
	
	private Date modificationDate;
	
	public Timestamp() {
		this(new Date(), new Date());
	}
	
	public Timestamp(Date creationDate, Date modificationDate) {
		this.creationDate = creationDate;
		this.modificationDate = modificationDate;
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
