package org.gama.safemodel.lang;

import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;
import org.gama.safemodel.description.MethodDescription;

/**
 * @author Guillaume Mary
 */
public class MetaDate<O extends MetaModel, M extends AbstractMemberDescription> extends MetaModel<O, M> {
	
	private MetaLong<MetaDate, MethodDescription<Long>> time = new MetaLong<>(method(String.class, "time", Long.class));
	
	public MetaDate(M description) {
		super(description);
		time.setOwner(this);
	}
	
	public MetaLong<MetaDate, MethodDescription<Long>> getTime() {
		return time;
	}
}
