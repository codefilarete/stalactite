package org.codefilarete.stalactite.engine.configurer.builder;

import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.stalactite.dsl.idpolicy.AlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification.SingleColumnIdentification;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.tool.collection.Iterables;

/**
 * Attach {@link SelectListener} for {@link AlreadyAssignedIdentifierPolicy} when relevant.
 *
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public class AlreadyAssignedMarkerStep<C, I> {
	
	void handleAlreadyAssignedMarker(AbstractIdentification<C, I> identification,
									 ConfiguredRelationalPersister<C, I> persister) {
		// when identifier policy is already-assigned one, we must ensure that entity is marked as persisted when it comes back from database
		// because user may forget to / can't mark it as such
		if (identification instanceof AbstractIdentification.SingleColumnIdentification && ((SingleColumnIdentification<C, I>) identification).getIdentifierPolicy() instanceof AlreadyAssignedIdentifierPolicy) {
			Consumer<C> asPersistedMarker = ((AlreadyAssignedIdentifierPolicy<C, I>) ((SingleColumnIdentification<C, I>) identification).getIdentifierPolicy()).getMarkAsPersistedFunction();
			persister.addSelectListener(new SelectListener<C, I>() {
				@Override
				public void afterSelect(Set<? extends C> result) {
					Iterables.filter(result, Objects::nonNull).forEach(asPersistedMarker);
				}
			});
		}
	}
}
