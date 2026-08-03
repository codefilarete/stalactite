package org.codefilarete.stalactite.engine.configurer.onetoone;

import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.builder.PostInitializer;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.cycle.OneToOneCycleLoader;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Container of {@link OneToOneRelationConfigurer}s of same entity type and their relation name (through {@link RelationConfigurer}).
 * Expected to exist as a one-per-entity-type.
 * 
 * As a {@link PostInitializer}, will invoke every registered {@link OneToOneRelationConfigurer}
 * {@link OneToOneConfigurerTemplate#configureWithSelectIn2Phases(String, ConfiguredRelationalEntityPersister, FirstPhaseCycleLoadListener) configureWithSelectIn2Phases method}
 * with a {@link OneToOneCycleLoader}.
 * 
 * @param <TRGT> type of all registered {@link OneToOneRelationConfigurer}
 */
public class OneToOneCycleConfigurer<TRGT> extends PostInitializer<TRGT> {
	
	// instantiated as a LinkedHashSet only for steady debugging purpose, could be replaced by a HashSet
	private final Set<RelationConfigurer<?, ?, ?, ?>> relations = new LinkedHashSet<>();
	
	public OneToOneCycleConfigurer(Class<TRGT> entityType) {
		super(entityType);
	}
	
	public <SRC, TRGTTABLE extends Table<TRGTTABLE>> void addCycleSolver(String relationIdentifier,
									 OneToOneConfigurerTemplate<SRC, TRGT, ?, ?, ?, TRGTTABLE, ?> oneToOneRelationConfigurer) {
		this.relations.add(new RelationConfigurer<>(relationIdentifier, oneToOneRelationConfigurer));
	}
	
	@Override
	public void consume(ConfiguredRelationalEntityPersister<TRGT, ?, ?> targetPersister) {
		registerRelationLoader(targetPersister);
	}
	
	private <SRC, TRGTID, TRGTTABLE extends Table<TRGTTABLE>> void registerRelationLoader(ConfiguredRelationalEntityPersister<TRGT, TRGTID, TRGTTABLE> targetPersister) {
		OneToOneCycleLoader<SRC, TRGT, TRGTID> oneToOneCycleLoader = new OneToOneCycleLoader<>(targetPersister);
		targetPersister.addSelectListener(oneToOneCycleLoader);
		relations.forEach(c -> {
			String tableAlias = c.relationName.replaceAll("\\W", "_");
			CascadeConfigurationResult<SRC, TRGT> configurationResult = ((RelationConfigurer<SRC, ?, TRGTID, TRGTTABLE>) c).oneToOneRelationConfigurer.configureWithSelectIn2Phases(
					tableAlias, targetPersister, oneToOneCycleLoader);
			oneToOneCycleLoader.addRelation(c.relationName, configurationResult);
		});
	}
	
	private class RelationConfigurer<SRC, SRCID, TRGTID, TRGTTABLE extends Table<TRGTTABLE>> {
		
		private final String relationName;
		private final OneToOneConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, ?, TRGTTABLE, ?> oneToOneRelationConfigurer;
		
		public RelationConfigurer(String relationName,
								  OneToOneConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, ?, TRGTTABLE, ?> oneToOneRelationConfigurer) {
			this.relationName = relationName;
			this.oneToOneRelationConfigurer = oneToOneRelationConfigurer;
		}
	}
}
