package org.gama.stalactite.persistence.engine.runtime.cycle;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.gama.stalactite.persistence.engine.configurer.CascadeManyConfigurer;
import org.gama.stalactite.persistence.engine.configurer.CascadeManyConfigurer.ConfigurationResult;
import org.gama.stalactite.persistence.engine.configurer.CascadeManyConfigurer.FirstPhaseCycleLoadListener;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.PostInitializer;
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredJoinedTablesPersister;

/**
 * Container of {@link CascadeManyConfigurer}s of same entity type and their relation name (throught {@link RelationConfigurer}).
 * Expected to exist as a one-per-entity-type.
 * 
 * As a {@link PostInitializer}, will invoke every registered {@link CascadeManyConfigurer}
 * {@link CascadeManyConfigurer#appendCascadesWith2PhasesSelect(String, IEntityConfiguredJoinedTablesPersister, FirstPhaseCycleLoadListener)} appendCascadesWith2PhasesSelect method}
 * with a {@link OneToManyCycleLoader}.
 * 
 * @param <TRGT> type of all registered {@link CascadeManyConfigurer}
 */
public class OneToManyCycleConfigurer<TRGT> extends PostInitializer<TRGT> {
	
	// instanciated as a LinkedHashSet only for steady debugging purpose, could be replaced by a HashSet
	private final Set<RelationConfigurer<?, ?, ?>> relations = new LinkedHashSet<>();
	
	public OneToManyCycleConfigurer(Class<TRGT> entityType) {
		super(entityType);
	}
	
	public <SRC> void addCycleSolver(String relationIdentifier,
									 CascadeManyConfigurer<SRC, TRGT, ?, ?, ? extends Collection<TRGT>> cascadeManyConfigurer) {
		this.relations.add(new RelationConfigurer<>(relationIdentifier, cascadeManyConfigurer));
	}
	
	@Override
	public void consume(IEntityConfiguredJoinedTablesPersister<TRGT, Object> targetPersister) {
		registerRelationLoader(targetPersister);
	}
	
	private <SRC, TRGTID> void registerRelationLoader(IEntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister) {
		OneToManyCycleLoader<SRC, TRGT, TRGTID> oneToManyCycleLoader = new OneToManyCycleLoader<>(targetPersister);
		targetPersister.addSelectListener(oneToManyCycleLoader);
		((Set<RelationConfigurer<SRC, ?, TRGTID>>) (Set) relations).forEach(c -> {
			String tableAlias = c.relationName.replaceAll("\\W", "_");
			ConfigurationResult<SRC, TRGT> configurationResult = c.cascadeManyConfigurer.appendCascadesWith2PhasesSelect(
					tableAlias, targetPersister, oneToManyCycleLoader.buildRowReader(c.relationName));
			oneToManyCycleLoader.addRelation(c.relationName, configurationResult);
		});
	}
	
	private class RelationConfigurer<SRC, SRCID, TRGTID> {
		
		private final String relationName;
		private final CascadeManyConfigurer<SRC, TRGT, SRCID, TRGTID, Collection<TRGT>> cascadeManyConfigurer;
		
		public RelationConfigurer(String relationName,
								  CascadeManyConfigurer<SRC, TRGT, SRCID, TRGTID, ? extends Collection<TRGT>> cascadeManyConfigurer) {
			this.relationName = relationName;
			this.cascadeManyConfigurer = (CascadeManyConfigurer<SRC, TRGT, SRCID, TRGTID, Collection<TRGT>>) cascadeManyConfigurer;
		}
	}
}