package org.codefilarete.stalactite.engine.runtime.cycle;

import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.runtime.SecondPhaseRelationLoader;
import org.codefilarete.stalactite.engine.configurer.onetomany.FirstPhaseCycleLoadListener;
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
public class OneToManyCycleLoader<SRC, TRGT, TRGTID> extends AbstractCycleLoader<SRC, TRGT, TRGTID> {
	
	public OneToManyCycleLoader(EntityPersister<TRGT, TRGTID> targetPersister) {
		super(targetPersister);
	}
	
	/**
	 * Implemented to read very first identifiers of source type
	 */
	public FirstPhaseCycleLoadListener<SRC, TRGTID> buildRowReader(String relationName) {
		return (src, targetId) -> {
			if (!SecondPhaseRelationLoader.isDefaultValue(targetId)) {
				OneToManyCycleLoader.this.currentRuntimeContext.get().addRelationToInitialize(relationName, src, targetId);
			}
		};
	}
	
	@Override
	protected void applyRelationToSource(EntityRelationStorage<SRC, TRGTID> relationStorage,
										 BeanRelationFixer<SRC, TRGT> beanRelationFixer,
										 Map<TRGTID, TRGT> targetPerId) {
		relationStorage.getEntitiesToFulFill().forEach(src -> {
			Set<TRGTID> trgtids = relationStorage.getRelationToInitialize(src);
			if (trgtids != null) {
				trgtids.forEach(targetId -> {
					// because we loop over all source instances of a recursion process (invoking SQL until no more entities need to be loaded),
					// and whereas this method is called only for one iteration of the whole process, we may encountered source that are not concerned
					// by given targetPerId, so we need to check for not null matching, else we would get a null element in target collection
					if (targetPerId.get(targetId) != null) {
						beanRelationFixer.apply(src, targetPerId.get(targetId));
					}
				});
			}
		});
	}
}
