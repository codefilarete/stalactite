package org.stalactite.lang.bean.safemodel.metamodel;

import org.stalactite.lang.bean.safemodel.MetaModel;

/**
 * @author Guillaume Mary
 */
public class MetaString<O extends MetaModel> extends MetaModel<O> {
	
	public MetaString(AbstractMemberDescription description) {
		super(description);
	}
	
	public MetaString(AbstractMemberDescription description, O owner) {
		super(description, owner);
	}
	
	public MetaModel charAt(int index) {
		MetaModel<MetaModel> chartAt = new MetaModel<>(method(String.class, "charAt", index));
		chartAt.setOwner(this);
		return chartAt;
	}
	
	public MetaModel charAt_array(int index) {
		MetaModel<MetaModel> toCharArray = new MetaModel<>(new ArrayDescription(String.class, "toCharArray()", index));
		toCharArray.setOwner(this);
		return toCharArray;
	}
	
}
