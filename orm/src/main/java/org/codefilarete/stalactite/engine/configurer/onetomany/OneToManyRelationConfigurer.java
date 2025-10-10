package org.codefilarete.stalactite.engine.configurer.onetomany;

import java.util.Collection;
import java.util.Set;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.dsl.naming.*;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl;
import org.codefilarete.stalactite.engine.configurer.AbstractRelationConfigurer;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * @param <SRC> type of input (left/source entities)
 * @param <TRGT> type of output (right/target entities)
 * @param <SRCID> identifier type of source entities
 * @param <TRGTID> identifier type of target entities
 * @author Guillaume Mary
 */
public class OneToManyRelationConfigurer<SRC, SRCID, TRGT, TRGTID> extends AbstractRelationConfigurer<SRC, SRCID, TRGT, TRGTID> {
	
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final JoinColumnNamingStrategy joinColumnNamingStrategy;
	private final AssociationTableNamingStrategy associationTableNamingStrategy;
	private final ColumnNamingStrategy indexColumnNamingStrategy;
	private final PrimaryKey<?, SRCID> leftPrimaryKey;
	
	public OneToManyRelationConfigurer(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									   Dialect dialect,
									   ConnectionConfiguration connectionConfiguration,
									   TableNamingStrategy tableNamingStrategy,
									   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
									   JoinColumnNamingStrategy joinColumnNamingStrategy,
									   AssociationTableNamingStrategy associationTableNamingStrategy,
									   ColumnNamingStrategy indexColumnNamingStrategy,
									   PersisterBuilderContext currentBuilderContext) {
		super(dialect, connectionConfiguration, sourcePersister, tableNamingStrategy, currentBuilderContext);
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.associationTableNamingStrategy = associationTableNamingStrategy;
		this.indexColumnNamingStrategy = indexColumnNamingStrategy;
		
		this.leftPrimaryKey = sourcePersister.getMapping().getTargetTable().getPrimaryKey();
	}
	
	public void configure(OneToManyRelation<SRC, TRGT, TRGTID, ? extends Collection<TRGT>> oneToManyRelation) {
		
		RelationMode maintenanceMode = oneToManyRelation.getRelationMode();
		// selection is always present (else configuration is nonsense !)
		boolean orphanRemoval = maintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL;
		boolean writeAuthorized = maintenanceMode != RelationMode.READ_ONLY;
		String columnName = oneToManyRelation.getIndexingColumnName();
		
		OneToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, ?, ?> associationConfiguration = new OneToManyAssociationConfiguration<>(oneToManyRelation,
				sourcePersister, leftPrimaryKey,
				foreignKeyNamingStrategy, joinColumnNamingStrategy, indexColumnNamingStrategy, columnName,
				orphanRemoval, writeAuthorized);
		OneToManyConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, ?, ?> configurer;
		if (oneToManyRelation.isOwnedByReverseSide()) {
			// case : reverse property is defined through one of the setter, getter or column on the reverse side
			if (maintenanceMode == RelationMode.ASSOCIATION_ONLY) {
				throw new MappingConfigurationException(RelationMode.ASSOCIATION_ONLY + " is only relevant with an association table");
			}
			configurer = new OneToManyWithMappedAssociationConfigurer<>(associationConfiguration, oneToManyRelation.isFetchSeparately());
		} else {
			configurer = new OneToManyWithAssociationTableConfigurer<>(associationConfiguration,
					oneToManyRelation.isFetchSeparately(),
					associationTableNamingStrategy,
					maintenanceMode == RelationMode.ASSOCIATION_ONLY,
					dialect,
					connectionConfiguration);
		}
		
		EntityMappingConfiguration<TRGT, TRGTID> targetMappingConfiguration = oneToManyRelation.getTargetMappingConfiguration();
		if (currentBuilderContext.isCycling(targetMappingConfiguration)) {
			// cycle detected
			// we had a second phase load because cycle can hardly be supported by simply joining things together because at one time we will
			// fall into infinite loop (think to SQL generation of a cycling graph ...)
			Class<TRGT> targetEntityType = targetMappingConfiguration.getEntityType();
			// adding the relation to an eventually already existing cycle configurer for the entity
			OneToManyCycleConfigurer<TRGT> cycleSolver = (OneToManyCycleConfigurer<TRGT>)
					Iterables.find(currentBuilderContext.getBuildLifeCycleListeners(), p -> p instanceof OneToManyCycleConfigurer && ((OneToManyCycleConfigurer<?>) p).getEntityType() == targetEntityType);
			if (cycleSolver == null) {
				cycleSolver = new OneToManyCycleConfigurer<>(targetEntityType);
				currentBuilderContext.addBuildLifeCycleListener(cycleSolver);
			}
			String relationName = AccessorDefinition.giveDefinition(oneToManyRelation.getCollectionProvider()).getName();
			cycleSolver.addCycleSolver(relationName, configurer);
		} else {
			Table targetTable = determineTargetTable(oneToManyRelation);
			ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister = new PersisterBuilderImpl<>(targetMappingConfiguration)
					.build(dialect, connectionConfiguration, targetTable);
			configurer.configure(targetPersister);
		}
	}
	
	private Table determineTargetTable(OneToManyRelation<SRC, TRGT, TRGTID, ?> oneToManyRelation) {
		Table reverseTable = nullable(oneToManyRelation.getReverseColumn()).map(Column::getTable).get();
		Table indexingTable = nullable(oneToManyRelation.getIndexingColumn()).map(Column::getTable).get();
		Set<Table> availableTables = Arrays.asHashSet(oneToManyRelation.getTargetTable(), reverseTable, indexingTable);
		availableTables.remove(null);
		if (availableTables.size() > 1) {
			class TableAppender extends StringAppender {
				@Override
				public StringAppender cat(Object o) {
					if (o instanceof Table) {
						return super.cat(((Table) o).getName());
					} else {
						return super.cat(o);
					}
				}
			}
			throw new MappingConfigurationException("Different tables used for configuring mapping : " + new TableAppender().ccat(availableTables, ", "));
		}
		
		// NB: even if no table is found in configuration, build(..) will create one
		Table result = nullable(oneToManyRelation.getTargetTable()).elseSet(reverseTable).elseSet(indexingTable).get();
		if (result == null) {
			result = lookupTableInRegisteredPersisters(oneToManyRelation.getTargetMappingConfiguration().getEntityType());
		}
		return result;
	}
}
