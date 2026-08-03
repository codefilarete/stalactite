package org.codefilarete.stalactite.engine.runtime.onetomany;

import java.util.Collection;

import org.codefilarete.stalactite.engine.configurer.onetomany.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalEntityPersister;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractOneToManyEngine<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>, SRCTABLE extends Table<SRCTABLE>, TRGTTABLE extends Table<TRGTTABLE>> {
	
	public static final int INDEXED_COLLECTION_FIRST_INDEX_VALUE = 1;
	
	protected final ConfiguredRelationalEntityPersister<SRC, SRCID, SRCTABLE> sourcePersister;
	
	protected final ConfiguredRelationalEntityPersister<TRGT, TRGTID, TRGTTABLE> targetPersister;
	
	protected final ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor;
	
	public AbstractOneToManyEngine(ConfiguredRelationalEntityPersister<SRC, SRCID, SRCTABLE> sourcePersister,
	                               ConfiguredRelationalEntityPersister<TRGT, TRGTID, TRGTTABLE> targetPersister,
	                               ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor) {
		this.sourcePersister = sourcePersister;
		this.targetPersister = targetPersister;
		this.manyRelationDescriptor = manyRelationDescriptor;
	}
	
	public abstract void addSelectCascadeIn2Phases(FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener);
	
	public abstract String addSelectCascade(boolean loadSeparately);
	
	public abstract void addInsertCascade(ConfiguredRelationalEntityPersister<TRGT, TRGTID, TRGTTABLE> targetPersister);
	
	public abstract void addUpdateCascade(ConfiguredRelationalEntityPersister<TRGT, TRGTID, TRGTTABLE> targetPersister);
	
	public abstract void addDeleteCascade(ConfiguredRelationalEntityPersister<TRGT, TRGTID, TRGTTABLE> targetPersister);
	
	public ManyRelationDescriptor<SRC, TRGT, C> getManyRelationDescriptor() {
		return manyRelationDescriptor;
	}
}
