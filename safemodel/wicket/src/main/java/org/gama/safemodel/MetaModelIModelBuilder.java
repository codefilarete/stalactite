package org.gama.safemodel;

import org.apache.wicket.model.ChainingModel;
import org.apache.wicket.model.IModel;
import org.gama.reflection.AccessorChain;
import org.gama.reflection.AccessorChainMutator;

/**
 * @author Guillaume Mary
 */
public class MetaModelIModelBuilder<T> implements IMetaModelTransformer<IModel<T>, MetaModel> {
	
	/** Any model object (which may or may not implement IModel) */
	private Object target;
	
	public MetaModelIModelBuilder() {
	}
	
	public MetaModelIModelBuilder(Object target) {
		this.target = target;
	}
	
	@Override
	public ChainingModel<T> transform(MetaModel metaModel) {
		MetaModelAccessorBuilder metaModelAccessorBuilder = new MetaModelAccessorBuilder();
		final AccessorChain<Object, T> accessorChain = metaModelAccessorBuilder.transform(metaModel);
		final AccessorChainMutator chainMutator = accessorChain.toMutator();
		return new ChainingModel<T>(this.target) {
			@Override
			public T getObject() {
				return accessorChain.get(super.getObject());
			}
			
			@Override
			public void setObject(T object) {
				chainMutator.set(super.getObject(), object);
			}
			
			@Override
			public void detach() {
				
			}
		};
	}

}
