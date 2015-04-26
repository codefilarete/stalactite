package org.stalactite.lang.bean.safemodel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.stalactite.lang.Reflections;
import org.stalactite.lang.bean.safemodel.MetaModel.ArrayDescription;
import org.stalactite.lang.bean.safemodel.MetaModel.FieldDescription;
import org.stalactite.lang.bean.safemodel.MetaModel.MethodDescription;
import org.stalactite.reflection.AccessorByField;
import org.stalactite.reflection.AccessorByMethod;
import org.stalactite.reflection.ArrayAccessor;
import org.stalactite.reflection.IAccessor;

/**
 * @author Guillaume Mary
 */
public class MetaModelAccessorBuilder implements IMetaModelTransformer<List<IAccessor>> {
	
	private List<IAccessor> accessorChain;
	
	@Override
	public List<IAccessor> transform(MetaModel metaModel) {
		Iterator<MetaModel> modelPathIterator = new MetaModelPathIterator(metaModel) {
			@Override
			protected void onFieldDescription(MetaModel model) {
				addFieldAccessor(model);
			}
			
			@Override
			protected void onMethodDescription(MetaModel model) {
				addMethodAccessor(model);
			}
			
			@Override
			protected void onArrayDescription(MetaModel model) {
				addArrayAccessor(model);
			}
		};
		accessorChain = new ArrayList<>(5);
		while (modelPathIterator.hasNext()) {
			modelPathIterator.next();
		}
//		if (Iterables.last(accessorChain) instanceof AccessorByMethod) {
//			accessorChain.set(accessorChain.size()-1, addAccessorMutator());
//		}
		return accessorChain;
	}
	
	private void addFieldAccessor(MetaModel model) {
		FieldDescription description = (FieldDescription) model.getDescription();
		this.accessorChain.add(new AccessorByField(Reflections.getField(description.getDeclaringClass(), description.getName())));
	}
	
	private void addMethodAccessor(MetaModel model) {
		MethodDescription description = (MethodDescription) model.getDescription();
		this.accessorChain.add(new AccessorByMethod(
				Reflections.getMethod(description.getDeclaringClass(), description.getName(), description.getParameterTypes())));
	}
	
//	private void addAccessorMutator(MethodDescription description) {
//		this.accessorChain.add(new AccessorByMethod(
//				Reflections.getMethod(description.getDeclaringClass(), description.getName(), description.getParameterTypes()),
//				null));
//	}
//	
	private void addArrayAccessor(MetaModel model) {
		ArrayDescription description = (ArrayDescription) model.getDescription();
		this.accessorChain.add(new ArrayAccessor((Integer) model.getMemberParameter()));
	}
}
