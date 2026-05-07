package org.codefilarete.stalactite.engine.runtime.onetomany;

import java.util.Collection;

import org.codefilarete.stalactite.engine.configurer.onetomany.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractOneToManyEngine<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>> {
	
	public static final int INDEXED_COLLECTION_FIRST_INDEX_VALUE = 1;
	
	protected final ConfiguredRelationalPersister<SRC, SRCID> sourcePersister;
	
	protected final ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister;
	
	protected final ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor;
	
	public AbstractOneToManyEngine(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
	                               ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
	                               ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor) {
		this.sourcePersister = sourcePersister;
		this.targetPersister = targetPersister;
		this.manyRelationDescriptor = manyRelationDescriptor;
	}
	
	public abstract void addSelectCascadeIn2Phases(FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener);
	
	public abstract String addSelectCascade(boolean loadSeparately);
	
	public abstract void addInsertCascade(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister);
	
	public abstract void addUpdateCascade(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister);
	
	public abstract void addDeleteCascade(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister);
	
	public ManyRelationDescriptor<SRC, TRGT, C> getManyRelationDescriptor() {
		return manyRelationDescriptor;
	}
}
