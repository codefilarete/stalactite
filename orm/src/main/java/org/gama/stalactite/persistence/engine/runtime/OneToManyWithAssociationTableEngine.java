package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.function.Function;

import org.gama.stalactite.persistence.engine.AssociationRecord;
import org.gama.stalactite.persistence.engine.AssociationRecordPersister;
import org.gama.stalactite.persistence.engine.AssociationTable;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>>
		extends AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, SRCID, TRGTID, C, AssociationRecord, AssociationTable> {
	
	public OneToManyWithAssociationTableEngine(JoinedTablesPersister<SRC, SRCID, ?> joinedTablesPersister,
											   JoinedTablesPersister<TRGT, TRGTID, ?> targetPersister,
											   ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor,
											   AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister) {
		super(joinedTablesPersister, targetPersister, manyRelationDescriptor, associationPersister);
	}
	
	@Override
	protected AssociationRecordInsertionCascader<SRC, TRGT, SRCID, TRGTID, C> newRecordInsertionCascader(
			Function<SRC, C> collectionGetter,
			AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister,
			IEntityMappingStrategy<SRC, SRCID, ?> mappingStrategy,
			IEntityMappingStrategy<TRGT, TRGTID, ?> targetStrategy) {
		return new AssociationRecordInsertionCascader<>(associationPersister, collectionGetter, mappingStrategy, targetStrategy);
	}
	
	@Override
	protected AssociationRecord newRecord(SRC e, TRGT target, int index) {
		return new AssociationRecord(joinedTablesPersister.getMappingStrategy().getId(e), targetPersister.getMappingStrategy().getId(target));
	}
}