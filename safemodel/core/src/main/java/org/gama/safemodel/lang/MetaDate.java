package org.gama.safemodel.lang;

import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;

/**
 * @author Guillaume Mary
 */
public class MetaDate<O extends MetaModel> extends MetaModel<O> {
	
	private MetaModel<MetaModel> time = new MetaModel<>(method(String.class, "time"));
	
	public MetaDate() {
		time.setOwner(this);
	}
	
	public MetaDate(AbstractMemberDescription description) {
		super(description);
	}
	
	public MetaDate(AbstractMemberDescription description, O owner) {
		super(description, owner);
	}
	
	public MetaModel getTime() {
		return time;
	}
}
