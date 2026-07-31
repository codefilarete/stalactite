package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

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
public class SecondPhaseSelectListener<SRC, SRCID> implements SelectListener<SRC, SRCID> {
	
	private final Function<? super SRC, SRCID> idProvider;
	
	private final SelectExecutor<?, SRCID> secondPhaseLoader;
	
	private final ThreadLocalStorage<?> relationStorage;
	
	private final BiConsumer<SRCID, SRC> relationSewer;
	
	/**
	 * @param idProvider gives the identifier of the entities that own the relation, used as criteria of the second-phase query
	 * @param secondPhaseLoader the loader of the second-phase query
	 * @param relationStorage the storage filled while the second-phase result set is read, initialized and released by this listener
	 * @param relationSewer sews the entities gathered in the storage onto their owner
	 */
	public SecondPhaseSelectListener(Function<? super SRC, SRCID> idProvider,
	                                 SelectExecutor<?, SRCID> secondPhaseLoader,
	                                 ThreadLocalStorage<?> relationStorage,
	                                 BiConsumer<SRCID, SRC> relationSewer) {
		this.idProvider = idProvider;
		this.secondPhaseLoader = secondPhaseLoader;
		this.relationStorage = relationStorage;
		this.relationSewer = relationSewer;
	}
	
	@Override
	public void afterSelect(Set<? extends SRC> result) {
		try {
			relationStorage.init();
			
			Map<SRCID, ? extends SRC> sourcePerId = Iterables.map(result, idProvider);
			
			// loading the relation: entities are collected in memory by the relation fixer of the loader join
			secondPhaseLoader.select(sourcePerId.keySet());
			
			// we sew the relations
			sourcePerId.forEach(relationSewer);
		} finally {
			// we remove the internal ThreadLocal
			relationStorage.clear();
		}
	}
}
