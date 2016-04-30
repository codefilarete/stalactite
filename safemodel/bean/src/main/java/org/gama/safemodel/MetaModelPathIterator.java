package org.gama.safemodel;

import java.util.ArrayList;
import java.util.Iterator;

import org.gama.lang.collection.ReadOnlyIterator;
import org.gama.safemodel.description.ArrayDescription;
import org.gama.safemodel.description.ContainerDescription;
import org.gama.safemodel.description.FieldDescription;
import org.gama.safemodel.description.MethodDescription;

/**
 * A class that iterates over {@link MetaModel}s. On each Specialized methods are invoked according to the type of 
 * 
 * @author Guillaume Mary
 */
public abstract class MetaModelPathIterator extends ReadOnlyIterator<MetaModel> {
	
	private final Iterator<MetaModel<? extends MetaModel, ? extends ContainerDescription>> modelPathIterator;
	
	/**
	 * Constructeur who you pass a "leaf" of a MetaModel. The iteration will be done from the root {@link MetaModel} to this leaf.
	 * 
	 * @param metaModel the last
	 */
	public MetaModelPathIterator(MetaModel<? extends MetaModel, ? extends ContainerDescription> metaModel) {
		this(buildMetaModelIterator(metaModel));
	}
	
	/**
	 * Detailed constructor whom you passed the exact {@link Iterator} you like.
	 * So you may use only a part of a {@link MetaModel} path, for instance.
	 * 
	 * @param modelPathIterator a {@link MetaModel} {@link Iterator}
	 */
	public MetaModelPathIterator(Iterator<MetaModel<? extends MetaModel, ? extends ContainerDescription>> modelPathIterator) {
		this.modelPathIterator = modelPathIterator;
	}
	
	@Override
	public boolean hasNext() {
		return modelPathIterator.hasNext();
	}
	
	@Override
	public MetaModel next() {
		MetaModel<? extends MetaModel, ? extends ContainerDescription> childModel = modelPathIterator.next();
		ContainerDescription description = childModel.getDescription();
		if (description instanceof FieldDescription) {
			onFieldDescription((MetaModel<MetaModel, FieldDescription>) childModel);
		} else if (description instanceof MethodDescription) {
			onMethodDescription((MetaModel<MetaModel, MethodDescription>) childModel);
		} else if (description instanceof ArrayDescription) {
			onArrayDescription((MetaModel<MetaModel, ArrayDescription>) childModel);
		}
		return childModel;
	}
	
	private static Iterator<MetaModel<? extends MetaModel, ? extends ContainerDescription>> buildMetaModelIterator(MetaModel<? extends MetaModel, ? extends ContainerDescription> metaModel) {
		// The passed argument is the last child, we must invert the relation to simplify path building
		ArrayList<MetaModel<? extends MetaModel, ? extends ContainerDescription>> modelPath = new ArrayList<>(10);
		MetaModel<? extends MetaModel, ? extends ContainerDescription> owner = metaModel;
		while (owner != null) {
			modelPath.add(0, owner);
			owner = owner.getOwner();
		}
		return modelPath.iterator();
	}
	
	protected abstract void onFieldDescription(MetaModel<MetaModel, FieldDescription> childModel);
	
	protected abstract void onMethodDescription(MetaModel<MetaModel, MethodDescription> childModel);
	
	protected abstract void onArrayDescription(MetaModel<MetaModel, ArrayDescription> childModel);
}
