package org.gama.safemodel;

import org.gama.lang.collection.ReadOnlyIterator;
import org.gama.safemodel.description.ContainerDescription;

/**
 * Iterator of {@link ContainerDescription} to ease building of MetaModel path. It recusrses until root MetaModel (no owner) starting from a leaf one.
 * This Iterator may generally (always) be used counter wise.
 * 
 * @author Guillaume Mary
 */
public class ContainerDescriptionPathIterator<C extends ContainerDescription> extends ReadOnlyIterator<C> {
	
	private MetaModel<? extends MetaModel, C> current;
	
	public ContainerDescriptionPathIterator(MetaModel<? extends MetaModel, C> leafMetaModel) {
		current = leafMetaModel;
	}
	
	@Override
	public boolean hasNext() {
		return current != null && current.getDescription() != null;
	}
	
	@Override
	public C next() {
		MetaModel next = current;
		current = current.getOwner();
		return (C) next.getDescription();
	}
}
