package org.gama.safemodel;

import java.util.Iterator;

import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.safemodel.description.*;

/**
 * A class that gives a printable description of the path of a MetaModel
 * 
 * @author Guillaume Mary
 */
public class MetaModelPathBuilder implements IMetaModelTransformer<String> {
	
	private StringAppender path;
	
	/**
	 * Transforms a {@link MetaModel} to a String, from the root {@link MetaModel} to the given argument. The result is the concatenation
	 * of all descriptions on the path.
	 * 
	 * @param metaModel a "leaf" {@link MetaModel}
	 * @return a String presentation of the {@link MetaModel} path
	 * @see #catField(MetaModel)
	 * @see #catMethod(MetaModel)
	 * @see #catArray(MetaModel) 
	 */
	@Override
	public String transform(MetaModel metaModel) {
		// we use an iterator bound to our catXXX method, so we'll only have to call its next() method to be notified for adhoc concatenation 
		Iterator<MetaModel<MetaModel, ? extends AbstractMemberDescription>> modelPathIterator
				= new MetaMemberPathIterator<MetaModel<MetaModel, ? extends AbstractMemberDescription>>((MetaModel<MetaModel, ? extends AbstractMemberDescription>) metaModel) {
			@Override
			protected void onFieldDescription(MetaModel<MetaModel, FieldDescription> model) {
				catField(model);
			}
			
			@Override
			protected void onMethodDescription(MetaModel<MetaModel, MethodDescription> model) {
				catMethod(model);
			}
			
			@Override
			protected void onArrayDescription(MetaModel<MetaModel, ArrayDescription> model) {
				catArray(model);
			}
		};
		path = new StringAppender(100);
		// we iterate over all elements, only in order to invoke dedicated onXXXX() iterator's methods
		while (modelPathIterator.hasNext()) {
			modelPathIterator.next();
		}
		// final finish
		if (path.charAt(0) == '.') {
			path.cutHead(1);
		}
		return path.toString();
	}
	
	protected void catField(MetaModel<MetaModel, FieldDescription> model) {
		path.cat(".", model.getDescription().getName());
	}
	
	protected void catMethod(MetaModel<MetaModel, MethodDescription> model) {
		path.cat(".");
		MethodDescription description = model.getDescription();
		path.cat(description.getName(), "(");
		Object memberParameter = model.getMemberParameter();
		if (memberParameter != null) {
			Object[] methodParams = memberParameter.getClass().isArray() ? (Object[]) memberParameter : new Object[] { memberParameter };
			if (!Arrays.isEmpty(methodParams)) {
				for (Object parameter : methodParams) {
					path.cat(parameter, ", ");
				}
				path.cutTail(2);
			}
		}
		path.cat(")");
	}
	
	protected void catArray(MetaModel<MetaModel, ArrayDescription> model) {
		path.cat("[", model.getMemberParameter(), "]");
	}
}
