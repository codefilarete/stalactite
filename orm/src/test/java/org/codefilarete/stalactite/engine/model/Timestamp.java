package org.codefilarete.stalactite.engine.model;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

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
	
	public Timestamp(LocalDateTime creationDate, LocalDateTime modificationDate) {
		this.creationDate = Date.from(creationDate.atZone(ZoneId.systemDefault()).toInstant());
		this.modificationDate = Date.from(modificationDate.atZone(ZoneId.systemDefault()).toInstant());
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
