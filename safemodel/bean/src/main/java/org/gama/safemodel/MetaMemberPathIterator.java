package org.gama.safemodel;

import java.util.Iterator;

import org.gama.safemodel.description.*;

/**
 * A class that iterates over {@link MetaModel}s. On each Specialized methods are invoked according to the type of 
 * 
 * @author Guillaume Mary
 */
public abstract class MetaMemberPathIterator<C extends MetaModel<MetaModel, ? extends AbstractMemberDescription>> extends MetaModelPathIterator<C> {
	
	/**
	 * Constructeur who you pass a "leaf" of a MetaModel. The iteration will be done from the root {@link MetaModel} to this leaf.
	 * 
	 * @param metaModel the last
	 */
	public MetaMemberPathIterator(C metaModel) {
		this(buildIteratorFromRootTo(metaModel));
	}
	
	/**
	 * Detailed constructor whom you passed the exact {@link Iterator} you like.
	 * So you may use only a part of a {@link MetaModel} path, for instance.
	 *
	 * @param modelPathIterator a {@link MetaModel} {@link Iterator}
	 */
	public MetaMemberPathIterator(Iterator<C> modelPathIterator) {
		super(modelPathIterator);
	}
	
	@Override
	public C next() {
		C childModel = super.next();
		AbstractMemberDescription description = childModel.getDescription();
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
