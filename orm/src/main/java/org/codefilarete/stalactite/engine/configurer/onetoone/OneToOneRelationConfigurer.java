package org.codefilarete.stalactite.engine.configurer.onetoone;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl;
import org.codefilarete.stalactite.engine.configurer.AbstractRelationConfigurer;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Wrapper for {@link OneToOneOwnedBySourceConfigurer} and {@link OneToOneOwnedByTargetConfigurer} to make them available for cycle.
 *
 * @param <C> type of input (left/source entities)
 * @param <I> identifier type of source entities
 * @param <TRGT> type of output (right/target entities)
 * @param <TRGTID>> identifier type of target entities
 * @author Guillaume Mary
 */
public class OneToOneRelationConfigurer<C, I, TRGT, TRGTID> extends AbstractRelationConfigurer<C, I, TRGT, TRGTID> {
	
	private final JoinColumnNamingStrategy joinColumnNamingStrategy;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	
	public OneToOneRelationConfigurer(Dialect dialect,
									  ConnectionConfiguration connectionConfiguration,
									  ConfiguredRelationalPersister<C, I> sourcePersister,
									  TableNamingStrategy tableNamingStrategy,
									  JoinColumnNamingStrategy joinColumnNamingStrategy,
									  ForeignKeyNamingStrategy foreignKeyNamingStrategy,
									  PersisterBuilderContext currentBuilderContext) {
		super(dialect, connectionConfiguration, sourcePersister, tableNamingStrategy, currentBuilderContext);
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
	}
	
	/**
	 * Setup given one-to-one relation by adding write cascade as well as creating appropriate joins in 
	 *
	 * @param oneToOneRelation the relation to be configured
	 */
	public void configure(OneToOneRelation<C, TRGT, TRGTID> oneToOneRelation) {
		OneToOneConfigurerTemplate<C, TRGT, I, TRGTID, ?, ?, I> configurer;
		if (oneToOneRelation.isRelationOwnedByTarget()) {
			configurer = new OneToOneOwnedByTargetConfigurer<>(sourcePersister, oneToOneRelation, joinColumnNamingStrategy, foreignKeyNamingStrategy, dialect, connectionConfiguration);
		} else {
			configurer = new OneToOneOwnedBySourceConfigurer<>(sourcePersister, oneToOneRelation, joinColumnNamingStrategy, foreignKeyNamingStrategy);
		}
		
		String relationName = AccessorDefinition.giveDefinition(oneToOneRelation.getTargetProvider()).getName();
		
		EntityMappingConfiguration<TRGT, TRGTID> targetMappingConfiguration = oneToOneRelation.getTargetMappingConfiguration();
		if (currentBuilderContext.isCycling(targetMappingConfiguration)) {
			// cycle detected
			// we had a second phase load because cycle can hardly be supported by simply joining things together because at one time we will
			// fall into infinite loop (think to SQL generation of a cycling graph ...)
			Class<TRGT> targetEntityType = targetMappingConfiguration.getEntityType();
			// adding the relation to an eventually already existing cycle configurer for the entity
			OneToOneCycleConfigurer<TRGT> cycleSolver = (OneToOneCycleConfigurer<TRGT>)
					Iterables.find(currentBuilderContext.getBuildLifeCycleListeners(), p -> p instanceof OneToOneCycleConfigurer && ((OneToOneCycleConfigurer<?>) p).getEntityType() == targetEntityType);
			if (cycleSolver == null) {
				cycleSolver = new OneToOneCycleConfigurer<>(targetEntityType);
				currentBuilderContext.addBuildLifeCycleListener(cycleSolver);
			}
			cycleSolver.addCycleSolver(relationName, configurer);
		} else {
			// please note that even if no table is found in configuration, build(..) will create one
			Table targetTable = determineTargetTable(oneToOneRelation);
			ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister = new PersisterBuilderImpl<>(targetMappingConfiguration)
					.build(dialect, connectionConfiguration, targetTable);
			configurer.configure(relationName, targetPersister, oneToOneRelation.isFetchSeparately());
		}
	}
	
	private Table determineTargetTable(OneToOneRelation<C, TRGT, TRGTID> oneToOneRelation) {
		EntityMappingConfiguration<TRGT, TRGTID> targetMappingConfiguration = oneToOneRelation.getTargetMappingConfiguration();
		Table targetTable = nullable(oneToOneRelation.getTargetTable()).getOr(nullable(oneToOneRelation.getReverseColumn()).map(Column::getTable).get());
		if (targetTable == null) {
			targetTable = lookupTableInRegisteredPersisters(targetMappingConfiguration.getEntityType());
		}
		return targetTable;
	}
}