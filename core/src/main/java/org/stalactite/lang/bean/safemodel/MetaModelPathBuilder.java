package org.stalactite.lang.bean.safemodel;

import java.util.ArrayList;
import java.util.Iterator;

import org.stalactite.lang.StringAppender;
import org.stalactite.lang.bean.safemodel.MetaModel.AbstractMemberDescription;
import org.stalactite.lang.bean.safemodel.MetaModel.ArrayDescription;
import org.stalactite.lang.bean.safemodel.MetaModel.MethodDescription;
import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.Iterables;

/**
 * @author Guillaume Mary
 */
public class MetaModelPathBuilder implements IMetaModelTransformer<String> {
	
	@Override
	public String transform(MetaModel metaModel) {
		Iterator<MetaModel> modelPathIterator = buildMetaModelIterator(metaModel);
		StringAppender path = new StringAppender(100);
		while (modelPathIterator.hasNext()) {
			MetaModel childModel = modelPathIterator.next();
			AbstractMemberDescription description = childModel.getDescription();
			path.cat(description.getName());
			if (description instanceof MethodDescription) {
				cat((MethodDescription) description, path);
			} else if (description instanceof ArrayDescription) {
				cat((ArrayDescription) description, path);
			}
			path.cat(".");
		}
		path.cutTail(1);
		return path.toString();
	}
	
	private Iterator<MetaModel> buildMetaModelIterator(MetaModel metaModel) {
		// le paramètre d'entrée est le dernier fils, il faut inverser la relation
		// pour se simplifier la construction du chemin
		ArrayList<MetaModel> modelPath = new ArrayList<>(10);
		MetaModel owner = metaModel;
		while (owner != null) {
			modelPath.add(owner);
			owner = owner.getOwner();
		}
		return Iterables.reverseIterator(modelPath);
	}
	
	private void cat(MethodDescription description, StringAppender path) {
		path.cat("(");
		Object[] methodParams = description.getParameters();
		if (!Arrays.isEmpty(methodParams)) {
			for (Object parameter : methodParams) {
				path.cat(String.valueOf(parameter), ", ");
			}
			path.cutTail(2);
		}
		path.cat(")");
	}
	
	private void cat(ArrayDescription description, StringAppender path) {
		path.cat("[", String.valueOf(description.getIndex()), "]");
	}
}
