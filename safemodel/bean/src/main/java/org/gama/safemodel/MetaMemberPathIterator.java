package org.gama.safemodel;

import java.util.Iterator;

import org.gama.safemodel.description.ArrayDescription;
import org.gama.safemodel.description.ContainerDescription;
import org.gama.safemodel.description.FieldDescription;
import org.gama.safemodel.description.MethodDescription;

/**
 * A class that iterates over {@link MetaModel}s. On each Specialized methods are invoked according to the type of 
 * 
 * @author Guillaume Mary
 */
public abstract class MetaMemberPathIterator extends MetaModelPathIterator {
	
	/**
	 * Constructeur who you pass a "leaf" of a MetaModel. The iteration will be done from the root {@link MetaModel} to this leaf.
	 * 
	 * @param metaModel the last
	 */
	public MetaMemberPathIterator(MetaModel metaModel) {
		this(buildIteratorFromRootTo(metaModel));
	}
	
	/**
	 * Detailed constructor whom you passed the exact {@link Iterator} you like.
	 * So you may use only a part of a {@link MetaModel} path, for instance.
	 * 
	 * @param modelPathIterator a {@link MetaModel} {@link Iterator}
	 */
	public MetaMemberPathIterator(Iterator<MetaModel<?, ?>> modelPathIterator) {
		super(modelPathIterator);
	}
	
	@Override
	public MetaModel next() {
		MetaModel childModel = super.next();
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
	
	protected abstract void onFieldDescription(MetaModel<MetaModel, FieldDescription> childModel);
	
	protected abstract void onMethodDescription(MetaModel<MetaModel, MethodDescription> childModel);
	
	protected abstract void onArrayDescription(MetaModel<MetaModel, ArrayDescription> childModel);
}
