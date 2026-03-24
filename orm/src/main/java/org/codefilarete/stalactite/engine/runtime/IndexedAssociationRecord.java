package org.codefilarete.stalactite.engine.runtime;

import java.util.Objects;

import org.codefilarete.reflection.DefaultReadWritePropertyAccessPoint;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;

/**
 * Same as {@link AssociationRecord} but with an index field to store indexed collections such as {@link java.util.List}
 * or {@link java.util.LinkedHashSet}
 * 
 * @author Guillaume Mary
 */
public class IndexedAssociationRecord extends AssociationRecord {
	
	public static final ReadWritePropertyAccessPoint<IndexedAssociationRecord, Object> LEFT_ACCESSOR = DefaultReadWritePropertyAccessPoint.fromMethodReference(IndexedAssociationRecord::getLeft, IndexedAssociationRecord::setLeft);
	public static final ReadWritePropertyAccessPoint<IndexedAssociationRecord, Object> RIGHT_ACCESSOR = DefaultReadWritePropertyAccessPoint.fromMethodReference(IndexedAssociationRecord::getRight, IndexedAssociationRecord::setRight);
	public static final ReadWritePropertyAccessPoint<IndexedAssociationRecord, Integer> INDEX_ACCESSOR = DefaultReadWritePropertyAccessPoint.fromMethodReference(IndexedAssociationRecord::getIndex, IndexedAssociationRecord::setIndex);
	
	private int index;
	
	public IndexedAssociationRecord() {
	}
	
	public IndexedAssociationRecord(Object leftValue, Object rightValue, int index) {
		super(leftValue, rightValue);
		this.index = index;
	}
	
	public int getIndex() {
		return index;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof IndexedAssociationRecord)) return false;
		IndexedAssociationRecord that = (IndexedAssociationRecord) o;
		return Objects.equals(left, that.left) &&
				Objects.equals(right, that.right) &&
				index == that.index;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(left, right, index);
	}
}
