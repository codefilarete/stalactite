package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.Collection;

import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractOneToManyEngine<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>> {
	
	public static final int INDEXED_COLLECTION_FIRST_INDEX_VALUE = 1;
	
	protected final EntityWriteExecutor<SRC, SRCID> sourcePersister;
	
	protected final EntityReadWriteExecutor<TRGT, TRGTID> targetPersister;
	
	protected final ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor;
	
	public AbstractOneToManyEngine(EntityWriteExecutor<SRC, SRCID> sourcePersister,
	                               EntityReadWriteExecutor<TRGT, TRGTID> targetPersister,
	                               ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor) {
		this.sourcePersister = sourcePersister;
		this.targetPersister = targetPersister;
		this.manyRelationDescriptor = manyRelationDescriptor;
	}
	
	public abstract void addInsertCascade(EntityReadWriteExecutor<TRGT, TRGTID> targetPersister);
	
	public abstract void addUpdateCascade(EntityReadWriteExecutor<TRGT, TRGTID> targetPersister);
	
	public abstract void addDeleteCascade(EntityWriteExecutor<TRGT, TRGTID> targetPersister);
	
	public ManyRelationDescriptor<SRC, TRGT, C> getManyRelationDescriptor() {
		return manyRelationDescriptor;
	}
}
