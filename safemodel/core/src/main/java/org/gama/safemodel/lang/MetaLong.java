package org.gama.safemodel.lang;

import org.gama.safemodel.MetaModel;
import org.gama.safemodel.description.AbstractMemberDescription;

/**
 * @author Guillaume Mary
 */
public class MetaLong<O extends MetaModel, M extends AbstractMemberDescription> extends MetaModel<O, M> {
	
	public MetaLong(M description) {
		super(description);
	}

}
