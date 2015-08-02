package org.gama.safemodel.lang;

import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;

/**
 * @author Guillaume Mary
 */
public class MetaString<O extends MetaModel> extends MetaModel<O> {
	
	private MetaModel<MetaModel> length = new MetaModel<>(method(String.class, "length"));
	
	public MetaString() {
		length.setOwner(this);
	}
	
	public MetaString(AbstractMemberDescription description) {
		super(description);
	}
	
	public MetaString(AbstractMemberDescription description, O owner) {
		super(description, owner);
	}
	
	public MetaModel length() {
		return length;
	}
}
