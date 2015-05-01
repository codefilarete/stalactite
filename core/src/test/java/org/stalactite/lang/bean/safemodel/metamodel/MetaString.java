package org.stalactite.lang.bean.safemodel.metamodel;

import org.stalactite.lang.bean.safemodel.MetaModel;

/**
 * @author Guillaume Mary
 */
public class MetaString<O extends MetaModel> extends MetaModel<O> {
	
	public MetaString() {
	}
	
	public MetaString(AbstractMemberDescription description) {
		super(description);
	}
	
	public MetaString(AbstractMemberDescription description, O owner) {
		super(description, owner);
	}
	
	public MetaModel charAt(int index) {
		MetaModel<MetaModel> chartAt = new MetaModel<>(method(String.class, "charAt", Integer.TYPE));
		chartAt.setParameter(index);
		chartAt.setOwner(this);
		return chartAt;
	}
	
	public MetaModel toCharArray(int index) {
		MetaModel<MetaModel> chartAt = new MetaModel<>(method(String.class, "toCharArray"));
		chartAt.setOwner(this);
		MetaModel<MetaModel> toCharArray = new MetaModel<>(new ArrayDescription(String.class));
		toCharArray.setOwner(chartAt);
		toCharArray.setParameter(index);
		return toCharArray;
	}
	
}
