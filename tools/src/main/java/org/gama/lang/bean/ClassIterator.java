package org.gama.lang.bean;

import org.gama.lang.collection.ReadOnlyIterator;

/**
 * Parcoureur de la hiérarchie d'une classe
 */
public class ClassIterator extends ReadOnlyIterator<Class> {
	
	private Class currentClass, topBoundAncestor;
	
	public ClassIterator(Class currentClass) {
		this(currentClass, Object.class);
	}
	
	public ClassIterator(Class currentClass, Class topBoundAncestor) {
		this.currentClass = currentClass;
		this.topBoundAncestor = topBoundAncestor;
	}
	
	@Override
	public boolean hasNext() {
		return currentClass != null && !currentClass.equals(topBoundAncestor);
	}
	
	@Override
	public Class next() {
		Class next = currentClass;
		currentClass = currentClass.getSuperclass();
		return next;
	}
}
