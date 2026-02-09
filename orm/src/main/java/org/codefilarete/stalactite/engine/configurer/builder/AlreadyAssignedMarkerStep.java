package org.codefilarete.stalactite.engine.configurer.builder;

import org.codefilarete.stalactite.dsl.idpolicy.AlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;

/**
 * Attach {@link SelectListener} for {@link AlreadyAssignedIdentifierPolicy} when relevant.
 *
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public class AlreadyAssignedMarkerStep<C, I> {
	
	void handleAlreadyAssignedMarker(ConfiguredRelationalPersister<C, I> persister) {
		// when identifier policy is already-assigned one, we must ensure that entity is marked as persisted when it comes back from database
		// because user may forget to / can't mark it as such
		IdentifierInsertionManager<C, I> identifierInsertionManager = persister.getMapping().getIdMapping().getIdentifierInsertionManager();
		if (identifierInsertionManager instanceof AlreadyAssignedIdentifierManager) {
			// Transferring identifier manager InsertListener to here
			persister.addInsertListener(((AlreadyAssignedIdentifierManager<C, I>) identifierInsertionManager).getInsertListener());
			persister.addSelectListener(((AlreadyAssignedIdentifierManager<C, I>) identifierInsertionManager).getSelectListener());
		}
	}
}
