package org.gama.safemodel.lang;

import java.util.Date;

import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;
import org.gama.safemodel.description.MethodDescription;

/**
 * @author Guillaume Mary
 */
public class MetaDate<O extends MetaModel, M extends AbstractMemberDescription> extends MetaModel<O, M> {
	
	private MetaLong<MetaDate, MethodDescription<Long>> time = new MetaLong<>(MethodDescription.method(Date.class, "getTime", Long.TYPE));
	
	public MetaDate(M description) {
		super(description);
		time.setOwner(this);
	}
	
	public MetaLong<MetaDate, MethodDescription<Long>> getTime() {
		return time;
	}
}
