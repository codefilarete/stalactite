package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.function.Function;

import org.gama.stalactite.persistence.engine.AssociationRecord;
import org.gama.stalactite.persistence.engine.AssociationRecordPersister;
import org.gama.stalactite.persistence.engine.AssociationTable;
import org.gama.stalactite.persistence.engine.IConfiguredPersister;
import org.gama.stalactite.persistence.engine.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithAssociationTableEngine<SRC, TRGT, ID, C extends Collection<TRGT>>
		extends AbstractOneToManyWithAssociationTableEngine<SRC, TRGT, ID, C, AssociationRecord, AssociationTable> {
	
	public OneToManyWithAssociationTableEngine(IConfiguredPersister<SRC, ID> sourcePersister,
											   IEntityConfiguredJoinedTablesPersister<TRGT, ID> targetPersister,
											   ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor,
											   AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister) {
		super(sourcePersister, targetPersister, manyRelationDescriptor, associationPersister);
	}
	
	@Override
	protected AssociationRecordInsertionCascader<SRC, TRGT, ID, C> newRecordInsertionCascader(
			Function<SRC, C> collectionGetter,
			AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister,
			IEntityMappingStrategy<SRC, ID, ?> mappingStrategy,
			IEntityMappingStrategy<TRGT, ID, ?> targetStrategy) {
		return new AssociationRecordInsertionCascader<>(associationPersister, collectionGetter, mappingStrategy, targetStrategy);
	}
	
	@Override
	protected AssociationRecord newRecord(SRC e, TRGT target, int index) {
		return new AssociationRecord(sourcePersister.getMappingStrategy().getId(e), targetPersister.getMappingStrategy().getId(target));
	}
}