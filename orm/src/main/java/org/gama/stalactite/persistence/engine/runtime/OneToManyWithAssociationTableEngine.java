package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.function.Function;

import org.gama.stalactite.persistence.engine.AssociationRecord;
import org.gama.stalactite.persistence.engine.AssociationRecordPersister;
import org.gama.stalactite.persistence.engine.AssociationTable;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithAssociationTableEngine<I extends Identified, O extends Identified, J extends Identifier, C extends Collection<O>>
		extends AbstractOneToManyWithAssociationTableEngine<I, O, J, C, AssociationRecord, AssociationTable> {
	
	public OneToManyWithAssociationTableEngine(PersisterListener<I, J> persisterListener,
											   Persister<O, J, ?> targetPersister,
											   ManyRelationDescriptor<I, O, C> manyRelationDescriptor,
											   AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister) {
		super(persisterListener, targetPersister, manyRelationDescriptor, associationPersister);
	}
	
	@Override
	protected AssociationRecordInsertionCascader<I, O, C> newRecordInsertionCascader(Function<I, C> collectionGetter,
																					 AssociationRecordPersister<AssociationRecord, AssociationTable> associationPersister) {
		return new AssociationRecordInsertionCascader<>(associationPersister, collectionGetter);
	}
	
	@Override
	protected AssociationRecord newRecord(I e, O target, int index) {
		return new AssociationRecord(e.getId(), target.getId());
	}
}