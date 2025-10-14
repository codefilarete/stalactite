package org.codefilarete.stalactite.engine.configurer.onetomany;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.builder.PostInitializer;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.cycle.OneToManyCycleLoader;

/**
 * Container of {@link OneToManyRelationConfigurer}s of same entity type and their relation name (through {@link RelationConfigurer}).
 * Expected to exist as a one-per-entity-type.
 * 
 * As a {@link PostInitializer}, will invoke every registered {@link OneToManyRelationConfigurer}
 * {@link OneToManyConfigurerTemplate#configureWithSelectIn2Phases(String, ConfiguredRelationalPersister, FirstPhaseCycleLoadListener)} configureWithSelectIn2Phases method}
 * with a {@link OneToManyCycleLoader}.
 * 
 * @param <TRGT> type of all registered {@link OneToManyRelationConfigurer}
 */
class OneToManyCycleConfigurer<TRGT> extends PostInitializer<TRGT> {
	
	// instantiated as a LinkedHashSet only for steady debugging purpose, could be replaced by a HashSet
	private final Set<RelationConfigurer<?, ?, ?>> relations = new LinkedHashSet<>();
	
	public OneToManyCycleConfigurer(Class<TRGT> entityType) {
		super(entityType);
	}
	
	public <SRC> void addCycleSolver(String relationIdentifier,
									 OneToManyConfigurerTemplate<SRC, TRGT, ?, ?, ? extends Collection<TRGT>, ?> oneToManyRelationConfigurer) {
		this.relations.add(new RelationConfigurer<>(relationIdentifier, oneToManyRelationConfigurer));
	}
	
	@Override
	public void consume(ConfiguredRelationalPersister<TRGT, ?> targetPersister) {
		registerRelationLoader(targetPersister);
	}
	
	private <SRC, TRGTID> void registerRelationLoader(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		OneToManyCycleLoader<SRC, TRGT, TRGTID> oneToManyCycleLoader = new OneToManyCycleLoader<>(targetPersister);
		targetPersister.addSelectListener(oneToManyCycleLoader);
		((Set<RelationConfigurer<SRC, ?, TRGTID>>) (Set) relations).forEach(c -> {
			String tableAlias = c.relationName.replaceAll("\\W", "_");
			CascadeConfigurationResult<SRC, TRGT> configurationResult = c.oneToManyRelationConfigurer.configureWithSelectIn2Phases(
					tableAlias, targetPersister, oneToManyCycleLoader.buildRowReader(c.relationName));
			oneToManyCycleLoader.addRelation(c.relationName, configurationResult);
		});
	}
	
	private class RelationConfigurer<SRC, SRCID, TRGTID> {
		
		private final String relationName;
		private final OneToManyConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, Collection<TRGT>, ?> oneToManyRelationConfigurer;
		
		public RelationConfigurer(String relationName,
								  OneToManyConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, ? extends Collection<TRGT>, ?> oneToManyRelationConfigurer) {
			this.relationName = relationName;
			this.oneToManyRelationConfigurer = (OneToManyConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, Collection<TRGT>, ?>) oneToManyRelationConfigurer;
		}
	}
}