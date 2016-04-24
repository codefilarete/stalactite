package org.gama.safemodel.description;

/**
 * A general description of an attribute's container.
 * 
 * @author Guillaume Mary
 */
public class ContainerDescription<T> {
	
	private final T declaringContainer;
	
	public ContainerDescription(T declaringContainer) {
		this.declaringContainer = declaringContainer;
	}
	
	public T getDeclaringContainer() {
		return declaringContainer;
	}
}
