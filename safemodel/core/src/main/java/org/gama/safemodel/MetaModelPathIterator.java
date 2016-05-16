package org.gama.safemodel;

import java.util.ArrayList;
import java.util.Iterator;

import org.gama.lang.collection.ReadOnlyIterator;

/**
 * A class that iterates over {@link MetaModel}s.
 * 
 * @author Guillaume Mary
 */
public abstract class MetaModelPathIterator<C extends MetaModel> extends ReadOnlyIterator<C> {
	
	private final Iterator<C> modelPathIterator;
	
	public static <T extends MetaModel> Iterator<T> buildIteratorFromRootTo(T leafMetaModel) {
		// The passed argument is the last child, we must invert the relation to simplify path building
		ArrayList<T> modelPath = new ArrayList<>(10);
		T owner = leafMetaModel;
		while (owner != null) {
			modelPath.add(0, owner);
			owner = (T) owner.getOwner();
		}
		return modelPath.iterator();
	}
	
	/**
	 * Constructeur whom you pass a "leaf" of a MetaModel. The iteration will be done from the root {@link MetaModel} to this leaf.
	 * 
	 * @param metaModel the last
	 */
	public MetaModelPathIterator(C metaModel) {
		this(buildIteratorFromRootTo(metaModel));
	}
	
	/**
	 * Detailed constructor whom you pass the exact {@link Iterator} you like.
	 * So you may use only a part of a {@link MetaModel} path, for instance.
	 * 
	 * @param modelPathIterator a {@link MetaModel} {@link Iterator}
	 */
	public MetaModelPathIterator(Iterator<C> modelPathIterator) {
		this.modelPathIterator = modelPathIterator;
	}
	
	@Override
	public boolean hasNext() {
		return modelPathIterator.hasNext();
	}
	
	@Override
	public C next() {
		return modelPathIterator.next();
	}
}
