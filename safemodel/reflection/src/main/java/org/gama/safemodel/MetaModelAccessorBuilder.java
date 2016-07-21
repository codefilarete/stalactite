package org.gama.safemodel;

import java.lang.reflect.Method;
import java.util.Iterator;

import org.gama.lang.Reflections;
import org.gama.reflection.AccessorByField;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.AccessorChain;
import org.gama.reflection.ArrayAccessor;
import org.gama.safemodel.description.AbstractMemberDescription;
import org.gama.safemodel.description.ArrayDescription;
import org.gama.safemodel.description.FieldDescription;
import org.gama.safemodel.description.MethodDescription;

/**
 * @author Guillaume Mary
 */
public class MetaModelAccessorBuilder<C, T> implements IMetaModelTransformer<AccessorChain<C, T>, MetaModel> {
	
	private AccessorChain<C, T> accessorChain;
	
	@Override
	public AccessorChain<C, T> transform(MetaModel metaModel) {
		Iterator<MetaModel<MetaModel, ? extends AbstractMemberDescription>> modelPathIterator
				= new MetaMemberPathIterator<MetaModel<MetaModel, ? extends AbstractMemberDescription>>((MetaModel<MetaModel, ? extends AbstractMemberDescription>) metaModel) {
			@Override
			protected void onFieldDescription(MetaModel<MetaModel, FieldDescription> model) {
				addFieldAccessor(model);
			}
			
			@Override
			protected void onMethodDescription(MetaModel<MetaModel, MethodDescription> model) {
				addMethodAccessor(model);
			}
			
			@Override
			protected void onArrayDescription(MetaModel<MetaModel, ArrayDescription> model) {
				addArrayAccessor(model);
			}
		};
		accessorChain = new AccessorChain<>();
		// we iterate over all elements, only in order to invoke dedicated onXXXX() iterator's methods
		while (modelPathIterator.hasNext()) {
			modelPathIterator.next();
		}
		return accessorChain;
	}
	
	private void addFieldAccessor(MetaModel<MetaModel, FieldDescription> model) {
		FieldDescription description = model.getDescription();
		this.accessorChain.add(new AccessorByField(Reflections.findField(description.getDeclaringClass(), description.getName())));
	}
	
	private void addMethodAccessor(MetaModel<MetaModel, MethodDescription> model) {
		MethodDescription description = model.getDescription();
		Method method = Reflections.findMethod(description.getDeclaringClass(), description.getName(), description.getParameterTypes());
		AccessorByMethod accessorByMethod;
		if (description.getParameterTypes().length > 0) {
			if (model.getMemberParameter() != null) {
				accessorByMethod = new AccessorByMethod<>(method);
				// Passing MetaModel arguments to Accessor
				accessorByMethod.setParameters(model.getMemberParameter());
			} else {
				throw new IllegalArgumentException("Expected parameter but none was given (" + description.getDeclaringClass().getName() + "." + description.getName() + ")");
			}
		} else {
			accessorByMethod = new AccessorByMethod(method);
		}
		this.accessorChain.add(accessorByMethod);
	}
	
	private void addArrayAccessor(MetaModel<MetaModel, ArrayDescription> model) {
		this.accessorChain.add(new ArrayAccessor<>((int) model.getMemberParameter()));
	}
}
