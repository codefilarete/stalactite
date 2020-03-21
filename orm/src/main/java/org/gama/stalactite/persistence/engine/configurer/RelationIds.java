package org.gama.stalactite.persistence.engine.configurer;

import java.util.function.Function;

import org.gama.stalactite.persistence.engine.ISelectExecutor;

/**
 * @author Guillaume Mary
 */
public class RelationIds<SRC, TRGT, TRGTID> {
	private final ISelectExecutor<TRGT, TRGTID> selectExecutor;
	private final Function<TRGT, TRGTID> idAccessor;
	private final SRC source;
	private final TRGTID targetId;
	
	
	RelationIds(ISelectExecutor<TRGT, TRGTID> selectExecutor, Function<TRGT, TRGTID> idAccessor, SRC source, TRGTID targetId) {
		this.selectExecutor = selectExecutor;
		this.idAccessor = idAccessor;
		this.source = source;
		this.targetId = targetId;
	}
	
	public ISelectExecutor<TRGT, TRGTID> getSelectExecutor() {
		return selectExecutor;
	}
	
	public Function<TRGT, TRGTID> getIdAccessor() {
		return idAccessor;
	}
	
	public SRC getSource() {
		return source;
	}
	
	public TRGTID getTargetId() {
		return targetId;
	}
}
