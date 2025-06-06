package org.codefilarete.stalactite.engine.runtime.cycle;

import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.configurer.onetoone.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.runtime.SecondPhaseRelationLoader;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

/**
 * Loader in case of same entity type present in entity graph as child of itself : A.b -> B.c -> C.a
 * Sibling is not the purpose of this class.
 * 
 * Implemented as such :
 * - very first query reads cycling entity type identifiers
 * - a secondary query is executed to load subgraph with above identifiers
 * 
 * @param <SRC> type of root entity graph
 * @param <TRGT> cycling entity type (the one to be loaded)
 * @param <TRGTID> cycling entity identifier type
 *     
 */
public class OneToOneCycleLoader<SRC, TRGT, TRGTID> extends AbstractCycleLoader<SRC, TRGT, TRGTID>
	implements FirstPhaseCycleLoadListener<SRC, TRGTID> {
	
	public OneToOneCycleLoader(EntityPersister<TRGT, TRGTID> targetPersister) {
		super(targetPersister);
	}
	
	/**
	 * Implemented to read very first identifiers of source type
	 */
	@Override
	public void onFirstPhaseRowRead(SRC src, TRGTID targetId) {
		if (!SecondPhaseRelationLoader.isDefaultValue(targetId)) {
			// TODO : we should know which relation is being filled instead of iterating over all of them, no ? see OneToManyCycleLoader
			this.relations.forEach((relationName, configurationResult) -> {
				if (configurationResult.getSourcePersister().getClassToPersist().isInstance(src)) {
					this.currentRuntimeContext.get().addRelationToInitialize(relationName, src, targetId);
				}
			});
		}
	}
	
	@Override
	protected void applyRelationToSource(EntityRelationStorage<SRC, TRGTID> relationStorage,
										 BeanRelationFixer<SRC, TRGT> beanRelationFixer,
										 Map<TRGTID, TRGT> targetPerId) {
		relationStorage.getEntitiesToFulFill().forEach(src -> {
			Set<TRGTID> trgtids = relationStorage.getRelationToInitialize(src);
			if (trgtids != null) {
				trgtids.forEach(targetId -> beanRelationFixer.apply(src, targetPerId.get(targetId)));
			}
		});
	}
}