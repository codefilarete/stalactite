package org.codefilarete.stalactite.mapping.id.manager;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.WriteExecutor.JDBCBatchingIterator;
import org.codefilarete.stalactite.mapping.SimpleIdMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.WriteOperation;

/**
 * Identifier manager to be used when identifier is already specified on entity, therefore this requires :
 * <ul>
 * <li>a way to know if entity is already present in database, this is left to a {@link Function}&lt;C, Boolean&gt;</li>
 * <li>the entity to be marked as persisted in any kind of way, this is left to a {@link Consumer}</li>
 * </ul>
 * 
 * A way o managing it can be to create a wrapper around identifier.
 * 
 * @author Guillaume Mary
 * @see SimpleIdMapping.IsNewDeterminer#isNew(Object)
 */
public class AlreadyAssignedIdentifierManager<C, I> implements IdentifierInsertionManager<C, I> {
	
	private final Class<I> identifierType;
	
	private final SetPersistedFlagAfterInsertListener setPersistedFlagAfterInsertListener = new SetPersistedFlagAfterInsertListener();
	
	private final SetPersistedFlagAfterSelectListener setPersistedFlagAfterSelectListener = new SetPersistedFlagAfterSelectListener();
	
	/** The {@link Consumer} that allows instances to be marked as persisted */
	private final Consumer<C> markAsPersistedFunction;
	
	/** The {@link Function} that allows to know if an instance is already persisted (expected to exist in database) */
	private final Function<C, Boolean> isPersistedFunction;
	
	public AlreadyAssignedIdentifierManager(Class<I> identifierType,
											Consumer<C> markAsPersistedFunction,
											Function<C, Boolean> isPersistedFunction) {
		this.identifierType = identifierType;
		this.markAsPersistedFunction = markAsPersistedFunction;
		this.isPersistedFunction = isPersistedFunction;
	}
	
	@Override
	public Class<I> getIdentifierType() {
		return identifierType;
	}
	
	public Consumer<C> getMarkAsPersistedFunction() {
		return markAsPersistedFunction;
	}
	
	public Function<C, Boolean> getIsPersistedFunction() {
		return isPersistedFunction;
	}
	
	@Override
	public JDBCBatchingIterator<C> buildJDBCBatchingIterator(Iterable<? extends C> entities, WriteOperation<? extends Column<? extends Table, ?>> writeOperation, int batchSize) {
		return new JDBCBatchingIterator<>(entities, writeOperation, batchSize);
	}
	
	@Override
	public InsertListener<C> getInsertListener() {
		return this.setPersistedFlagAfterInsertListener;
	}
	
	@Override
	public SelectListener<C, I> getSelectListener() {
		return this.setPersistedFlagAfterSelectListener;
	}
	
	public void setPersistedFlag(C e) {
		if (markAsPersistedFunction != null ) {
			markAsPersistedFunction.accept(e);
		}
	}
	
	private class SetPersistedFlagAfterInsertListener implements InsertListener<C> {
		
		@Override
		public void afterInsert(Iterable<? extends C> entities) {
			markAsPersisted(entities);
		}
	}
	
	private class SetPersistedFlagAfterSelectListener implements SelectListener<C, I> {
		
		@Override
		public void afterSelect(Set<? extends C> entities) {
			markAsPersisted(entities);
		}
	}
	
	/**
	 * Massive version of {@link #setPersistedFlag(Object)}, made to avoid code duplicate between {@link SetPersistedFlagAfterInsertListener}
	 * and {@link SetPersistedFlagAfterSelectListener}
	 *
	 * @param entities entities to be marked as persisted
	 */
	protected void markAsPersisted(Iterable<? extends C> entities) {
		for (C e : entities) {
			setPersistedFlag(e);
		}
	}
}
