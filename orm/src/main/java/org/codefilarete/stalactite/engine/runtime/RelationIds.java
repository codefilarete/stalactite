package org.codefilarete.stalactite.engine.runtime;

import java.util.Objects;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.SelectExecutor;

/**
 * @author Guillaume Mary
 */
class RelationIds<SRC, TRGT, TRGTID> {
	private final SelectExecutor<TRGT, TRGTID> selectExecutor;
	private final Function<TRGT, TRGTID> idAccessor;
	private final SRC source;
	private final TRGTID targetId;
	
	
	RelationIds(SelectExecutor<TRGT, TRGTID> selectExecutor, Function<TRGT, TRGTID> idAccessor, SRC source, TRGTID targetId) {
		this.selectExecutor = selectExecutor;
		this.idAccessor = idAccessor;
		this.source = source;
		this.targetId = targetId;
	}
	
	public SelectExecutor<TRGT, TRGTID> getSelectExecutor() {
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
	
	/**
	 * Implemented to stabilize {@link RelationIds} in sets to have steady orders of SQL statements
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof RelationIds)) return false;
		
		RelationIds<?, ?, ?> that = (RelationIds<?, ?, ?>) o;
		
		if (!selectExecutor.equals(that.selectExecutor)) return false;
		if (!Objects.equals(source, that.source)) return false;
		return Objects.equals(targetId, that.targetId);
	}
	
	/**
	 * Implemented to stabilize {@link RelationIds} in sets to have steady orders of SQL statements
	 */
	@Override
	public int hashCode() {
		int result = selectExecutor.hashCode();
		result = 31 * result + (source != null ? source.hashCode() : 0);
		result = 31 * result + (targetId != null ? targetId.hashCode() : 0);
		return result;
	}
}
