package org.stalactite.lang.bean.safemodel;

import org.stalactite.lang.StringAppender;
import org.stalactite.lang.bean.safemodel.MetaModel.AbstractMemberDescription;
import org.stalactite.lang.bean.safemodel.MetaModel.MethodDescription;

/**
 * @author Guillaume Mary
 */
public class MetaModelPathBuilder implements IMetaModelTransformer<String> {
	
	@Override
	public String transform(MetaModel metaModel) {
		StringAppender path = new StringAppender(100);
		MetaModel owner = metaModel;
		while (owner != null && owner.getDescription() != null) {
			AbstractMemberDescription description = owner.getDescription();
			StringAppender accessorDescription = new StringAppender(50);
			accessorDescription.cat(description.getName());
			if (description instanceof MethodDescription) {
				accessorDescription.cat("(");
				Object[] methodParams = ((MethodDescription) description).getParameters();
				for (Object parameter : methodParams) {
					accessorDescription.cat(String.valueOf(parameter), ", ");
				}
				if (methodParams.length > 0) {
					accessorDescription.cutTail(2);
				}
				accessorDescription.cat(")");
			}
			path.cat(0, ".", accessorDescription);
			owner = owner.getOwner();
		}
		path.cutHead(1);
		return path.toString();
	}
}
