package org.codefilarete.stalactite.engine.configurer.elementcollection;

import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.diff.CollectionDiffer;

/**
 * Represents a line in table storage, acts as a wrapper of an indexed element collection with source bean identifier addition.
 *
 * @param <TRGT> raw value type (element collection type)
 * @param <ID> source bean identifier type
 */
public class IndexedElementRecord<TRGT, ID> extends ElementRecord<TRGT, ID> {
	
	public static final PropertyAccessor<IndexedElementRecord<Object, Object>, Integer> INDEX_ACCESSOR = PropertyAccessor.fromMethodReference(
			IndexedElementRecord::getIndex,
			IndexedElementRecord::setIndex);
	
	private int index;
	
	/**
	 * Default constructor for select instantiation
	 */
	public IndexedElementRecord() {
	}
	
	public IndexedElementRecord(ID id, TRGT element, int index) {
		super(id, element);
		this.index = index;
	}
	
	public int getIndex() {
		return index;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	/**
	 * Identifier for {@link CollectionDiffer} support (update use case), because it compares beans
	 * through their "footprint" which is their id in default/entity case, but since we are a value type, we must provide a dedicated footprint.
	 * Could be hashCode() if it was implemented on identifier + element, but implementing it would require implementing equals() (to comply
	 * with best practices) which is not our case nor required by {@link CollectionDiffer}.
	 * Note : name of this method is not important
	 */
	public int footprint() {
		return super.footprint() + 31 * index;
	}
}
