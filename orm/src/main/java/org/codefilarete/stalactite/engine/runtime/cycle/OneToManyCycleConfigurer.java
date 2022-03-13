package org.codefilarete.stalactite.engine.runtime.cycle;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.CascadeManyConfigurer;
import org.codefilarete.stalactite.engine.configurer.CascadeManyConfigurer.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.PostInitializer;

/**
 * Container of {@link CascadeManyConfigurer}s of same entity type and their relation name (through {@link RelationConfigurer}).
 * Expected to exist as a one-per-entity-type.
 * 
 * As a {@link PostInitializer}, will invoke every registered {@link CascadeManyConfigurer}
 * {@link CascadeManyConfigurer#appendCascadesWith2PhasesSelect(String, EntityConfiguredJoinedTablesPersister, FirstPhaseCycleLoadListener)} appendCascadesWith2PhasesSelect method}
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
	public void consume(EntityConfiguredJoinedTablesPersister<TRGT, Object> targetPersister) {
		registerRelationLoader(targetPersister);
	}
	
	private <SRC, TRGTID> void registerRelationLoader(EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister) {
		OneToManyCycleLoader<SRC, TRGT, TRGTID> oneToManyCycleLoader = new OneToManyCycleLoader<>(targetPersister);
		targetPersister.addSelectListener(oneToManyCycleLoader);
		((Set<RelationConfigurer<SRC, ?, TRGTID>>) (Set) relations).forEach(c -> {
			String tableAlias = c.relationName.replaceAll("\\W", "_");
			CascadeConfigurationResult<SRC, TRGT> configurationResult = c.cascadeManyConfigurer.appendCascadesWith2PhasesSelect(
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