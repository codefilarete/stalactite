package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.tool.collection.Iterables;

/**
 * {@link SelectListener} that triggers the second phase of a separate-fetch load : once the entities that own the
 * relation are loaded (first phase), it runs the given loader on their identifiers (second phase), then sews the
 * loaded relation onto its owner.
 * <p>
 * Entities of the relation are not returned by the loader in a shape that can be directly sewn (the owner is not part
 * of the second-phase query), hence they are gathered by a {@link ThreadLocalStorage} while the result set is read :
 * this class takes care of its lifecycle, and delegates the sewing itself to the given {@link BiConsumer}, which is
 * expected to read that very storage.
 * <p>
 * Note that this is not a lazy loading : no query is triggered in the background when accessing the relation,
 * everything is loaded eagerly so that the whole aggregate is available and coherent when returned.
 *
 * @param <SRC> type of the entity that owns the relation
 * @param <SRCID> identifier type of the entity that owns the relation
 * @author Guillaume Mary
 * @see AssociationTableLoader
 */
public class SecondPhaseRelationSelectListener<SRC, TRGT, SRCID, TRGTID> implements SelectListener<SRC, SRCID> {
	
	private final ThreadLocalStorage<SRC, Set<TRGTID>, RelationStorage<SRC, TRGTID>> relationIdsHolder;
	
	private final Function<TRGT, TRGTID> idProvider;
	
	private final SelectExecutor<TRGT, TRGTID> secondPhaseLoader;
	
	private final BiConsumer<SRC, TRGT> relationSewer;
	
	/**
	 * Creates a listener that coordinates the two-phase loading of a relation.
	 * @param relationIdsHolder holder for relation identifiers collected in the first phase
	 * @param idProvider function used to extract an identifier from a loaded target entity
	 * @param secondPhaseLoader loader used to fetch all target entities in the second phase
	 * @param relationSewer callback used to attach a loaded target entity to its owning source entity
	 */
	public SecondPhaseRelationSelectListener(ThreadLocalStorage<SRC, Set<TRGTID>, RelationStorage<SRC, TRGTID>> relationIdsHolder,
											 Function<TRGT, TRGTID> idProvider,
											 SelectExecutor<TRGT, TRGTID> secondPhaseLoader,
											 BiConsumer<SRC, TRGT> relationSewer) {
		this.relationIdsHolder = relationIdsHolder;
		this.idProvider = idProvider;
		this.secondPhaseLoader = secondPhaseLoader;
		this.relationSewer = relationSewer;
	}
	
	@Override
	public void beforeSelect(Iterable<SRCID> ids) {
		relationIdsHolder.init();
	}
	
	@Override
	public void afterSelect(Set<? extends SRC> result) {
		// we load all the target entities (of all sources, for efficiency)
		Map<SRC, Set<TRGTID>> targetIdPerSource = relationIdsHolder.getStorage().getTargetIdPerSource();
		Set<TRGTID> targetIds = targetIdPerSource.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
		if (!targetIds.isEmpty()) {	// we only avoid some extra work if there's nothing to do
			Set<TRGT> targets = secondPhaseLoader.select(targetIds);
			Map<TRGTID, TRGT> targetPerId = Iterables.map(targets, idProvider);
			
			// we sew the relations
			result.forEach(src -> {
				// filling the collection
				Set<TRGTID> sourceTargetId = targetIdPerSource.get(src);
				if (sourceTargetId != null) {  // sourceTargetId can be null if there's no associated entity in the database
					sourceTargetId.stream()
							.map(targetPerId::get)
							.forEach(target -> relationSewer.accept(src, target));
				}
			});
		}
		// we remove the internal ThreadLocal
		releaseRelationHolder();
	}
	
	@Override
	public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
		releaseRelationHolder();
	}
	
	private void releaseRelationHolder() {
		relationIdsHolder.clear();
	}
}
