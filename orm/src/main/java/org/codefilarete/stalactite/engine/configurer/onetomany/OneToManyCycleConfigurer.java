package org.codefilarete.stalactite.engine.configurer.onetomany;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.builder.PostInitializer;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.cycle.OneToManyCycleLoader;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Container of {@link OneToManyRelationConfigurer}s of same entity type and their relation name (through {@link RelationConfigurer}).
 * Expected to exist as a one-per-entity-type.
 * 
 * As a {@link PostInitializer}, will invoke every registered {@link OneToManyRelationConfigurer}
 * {@link OneToManyConfigurerTemplate#configureWithSelectIn2Phases(String, ConfiguredRelationalEntityPersister, FirstPhaseCycleLoadListener)} configureWithSelectIn2Phases method}
 * with a {@link OneToManyCycleLoader}.
 * 
 * @param <TRGT> type of all registered {@link OneToManyRelationConfigurer}
 */
class OneToManyCycleConfigurer<TRGT> extends PostInitializer<TRGT> {
	
	// instantiated as a LinkedHashSet only for steady debugging purpose, could be replaced by a HashSet
	private final Set<RelationConfigurer<?, ?, ?, ?>> relations = new LinkedHashSet<>();
	
	public OneToManyCycleConfigurer(Class<TRGT> entityType) {
		super(entityType);
	}
	
	public <SRC> void addCycleSolver(String relationIdentifier,
									 OneToManyConfigurerTemplate<SRC, TRGT, ?, ?, ? extends Collection<TRGT>, ?, ?> oneToManyRelationConfigurer) {
		this.relations.add(new RelationConfigurer<>(relationIdentifier, oneToManyRelationConfigurer));
	}
	
	@Override
	public void consume(ConfiguredRelationalEntityPersister<TRGT, ?, ?> targetPersister) {
		registerRelationLoader(targetPersister);
	}
	
	private <SRC, TRGTID, TRGTTABLE extends Table<TRGTTABLE>> void registerRelationLoader(ConfiguredRelationalEntityPersister<TRGT, TRGTID, TRGTTABLE> targetPersister) {
		OneToManyCycleLoader<SRC, TRGT, TRGTID> oneToManyCycleLoader = new OneToManyCycleLoader<>(targetPersister);
		targetPersister.addSelectListener(oneToManyCycleLoader);
		((Set<RelationConfigurer<SRC, ?, TRGTID, TRGTTABLE>>) (Set) relations).forEach(c -> {
			String tableAlias = c.relationName.replaceAll("\\W", "_");
			CascadeConfigurationResult<SRC, TRGT> configurationResult = c.oneToManyRelationConfigurer.configureWithSelectIn2Phases(
					tableAlias, targetPersister, oneToManyCycleLoader.buildRowReader(c.relationName));
			oneToManyCycleLoader.addRelation(c.relationName, configurationResult);
		});
	}
	
	private class RelationConfigurer<SRC, SRCID, TRGTID, TRGTTABLE extends Table<TRGTTABLE>> {
		
		private final String relationName;
		private final OneToManyConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, Collection<TRGT>, ?, TRGTTABLE> oneToManyRelationConfigurer;
		
		public RelationConfigurer(String relationName,
								  OneToManyConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, ? extends Collection<TRGT>, ?, TRGTTABLE> oneToManyRelationConfigurer) {
			this.relationName = relationName;
			this.oneToManyRelationConfigurer = (OneToManyConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, Collection<TRGT>, ?, TRGTTABLE>) oneToManyRelationConfigurer;
		}
	}
}
