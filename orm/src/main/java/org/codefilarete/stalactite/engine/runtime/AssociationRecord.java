package org.codefilarete.stalactite.engine.runtime;

import java.util.Objects;

import org.codefilarete.reflection.ReversibleAccessor;

import static org.codefilarete.reflection.Accessors.accessorByMethodReference;

/**
 * A container to store association table rows.
 * It will be used in one-to-many associations and as no reason to be used outside of it. 
 * 
 * @author Guillaume Mary
 */
public class AssociationRecord {
	
	public static final ReversibleAccessor<AssociationRecord, Object> LEFT_ACCESSOR = accessorByMethodReference(AssociationRecord::getLeft, AssociationRecord::setLeft);
	public static final ReversibleAccessor<AssociationRecord, Object> RIGHT_ACCESSOR = accessorByMethodReference(AssociationRecord::getRight, AssociationRecord::setRight);
	
	private Object left;
	private Object right;
	private boolean persisted = false;
	
	public AssociationRecord() {
	}
	
	public AssociationRecord(Object leftValue, Object rightValue) {
		this.left = leftValue;
		this.right = rightValue;
	}
	
	public Object getLeft() {
		return left;
	}
	
	public void setLeft(Object left) {
		this.left = left;
	}
	
	public Object getRight() {
		return right;
	}
	
	public void setRight(Object right) {
		this.right = right;
	}
	
	public void markAsPersisted() {
		persisted = true;
	}
	
	public boolean isPersisted() {
		return persisted;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof AssociationRecord)) return false;
		AssociationRecord that = (AssociationRecord) o;
		return Objects.equals(left, that.left) &&
				Objects.equals(right, that.right);
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(left, right);
	}
}
