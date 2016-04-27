package org.gama.safemodel.lang;

import org.gama.safemodel.MetaMember;
import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;
import org.gama.safemodel.description.MethodDescription;

import java.util.Date;

/**
 * @author Guillaume Mary
 */
public class MetaDate<O extends MetaModel, M extends AbstractMemberDescription> extends MetaModel<O, M> {
	
	private MetaLong<MetaDate, MethodDescription<Long>> time = new MetaLong<>(MetaMember.method(Date.class, "getTime", Long.TYPE));
	
	public MetaDate(M description) {
		super(description);
		time.setOwner(this);
	}
	
	public MetaLong<MetaDate, MethodDescription<Long>> getTime() {
		return time;
	}
}
