package org.gama.safemodel;

import java.util.Iterator;

import org.gama.lang.StringAppender;
import org.gama.safemodel.MetaModel.FieldDescription;
import org.gama.safemodel.MetaModel.MethodDescription;
import org.gama.lang.collection.Arrays;

/**
 * @author Guillaume Mary
 */
public class MetaModelPathBuilder implements IMetaModelTransformer<String> {
	
	private StringAppender path;
	
	@Override
	public String transform(MetaModel metaModel) {
		Iterator<MetaModel> modelPathIterator = new MetaModelPathIterator(metaModel) {
			@Override
			protected void onFieldDescription(MetaModel model) {
				catField(model);
			}
			
			@Override
			protected void onMethodDescription(MetaModel model) {
				catMethod(model);
			}
			
			@Override
			protected void onArrayDescription(MetaModel model) {
				catArray(model);
			}
		};
		path = new StringAppender(100);
		while (modelPathIterator.hasNext()) {
			modelPathIterator.next();
		}
		if (path.charAt(0) == '.') {
			path.cutHead(1);
		}
		return path.toString();
	}
	
	protected void catField(MetaModel model) {
		path.cat(".");
		path.cat(((FieldDescription) model.getDescription()).getName());
	}
	
	protected void catMethod(MetaModel model) {
		path.cat(".");
		MethodDescription description = (MethodDescription) model.getDescription();
		path.cat(description.getName());
		path.cat("(");
		Object memberParameter = model.getMemberParameter();
		if (memberParameter != null) {
			Object[] methodParams = memberParameter.getClass().isArray() ? (Object[]) memberParameter : new Object[]{memberParameter};
			if (!Arrays.isEmpty(methodParams)) {
				for (Object parameter : methodParams) {
					path.cat(String.valueOf(parameter), ", ");
				}
				path.cutTail(2);
			}
		}
		path.cat(")");
	}
	
	protected void catArray(MetaModel model) {
		path.cat("[", String.valueOf(model.getMemberParameter()), "]");
	}
}
