package org.codefilarete.stalactite.engine.configurer.elementcollection;

import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.diff.CollectionDiffer;

/**
 * Represents a line in table storage, acts as a wrapper of element collection with source bean identifier addition.
 *
 * @param <TRGT> raw value type (element collection type)
 * @param <ID> source bean identifier type
 */
class ElementRecord<TRGT, ID> {
	
	static final PropertyAccessor<ElementRecord<Object, Object>, Object> IDENTIFIER_ACCESSOR = PropertyAccessor.fromMethodReference(
			ElementRecord::getId,
			ElementRecord::setId);
	
	static final PropertyAccessor<ElementRecord<Object, Object>, Object> ELEMENT_ACCESSOR = PropertyAccessor.fromMethodReference(
			ElementRecord::getElement,
			ElementRecord::setElement);
	
	
	private ID id;
	private TRGT element;
	private boolean persisted = false;
	
	/**
	 * Default constructor for select instantiation
	 */
	public ElementRecord() {
	}
	
	public ElementRecord(ID id, TRGT element) {
		setId(id);
		this.element = element;
	}
	
	public boolean isNew() {
		return !persisted;
	}
	
	public boolean isPersisted() {
		return persisted;
	}
	
	public void markAsPersisted() {
		this.persisted = true;
	}
	
	public ElementRecord<TRGT, ID> setPersisted(boolean persisted) {
		this.persisted = persisted;
		return this;
	}
	
	public ID getId() {
		return id;
	}
	
	public void setId(ID id) {
		this.id = id;
		this.persisted = true;
	}
	
	public TRGT getElement() {
		return element;
	}
	
	public void setElement(TRGT element) {
		this.element = element;
	}
	
	/**
	 * Identifier for {@link CollectionDiffer} support (update use case), because it compares beans
	 * through their "footprint" which is their id in default/entity case, but since we are a value type, we must provide a dedicated footprint.
	 * Could be hashCode() if it was implemented on identifier + element, but implementing it would require implementing equals() (to comply
	 * with best practices) which is not our case nor required by {@link CollectionDiffer}.
	 * Note : name of this method is not important
	 */
	public int footprint() {
		int result = id.hashCode();
		result = 31 * result + element.hashCode();
		return result;
	}
}
