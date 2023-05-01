package org.codefilarete.stalactite.engine.configurer.onetomany;

import java.util.Collection;
import java.util.Set;

import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Arrays;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * @param <SRC> type of input (left/source entities)
 * @param <TRGT> type of output (right/target entities)
 * @param <SRCID> identifier type of source entities
 * @param <TRGTID> identifier type of target entities
 * @param <C> collection type of the relation
 * @author Guillaume Mary
 */
public class OneToManyRelationConfigurer<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>> {
	
	private final OneToManyRelation<SRC, TRGT, TRGTID, C> oneToManyRelation;
	private final ConfiguredRelationalPersister<SRC, SRCID> sourcePersister;
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	private final PersisterRegistry persisterRegistry;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final JoinColumnNamingStrategy joinColumnNamingStrategy;
	private final AssociationTableNamingStrategy associationTableNamingStrategy;
	private final ColumnNamingStrategy indexColumnNamingStrategy;
	
	public OneToManyRelationConfigurer(OneToManyRelation<SRC, TRGT, TRGTID, C> oneToManyRelation,
									   ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									   Dialect dialect,
									   ConnectionConfiguration connectionConfiguration,
									   PersisterRegistry persisterRegistry,
									   ForeignKeyNamingStrategy foreignKeyNamingStrategy,
									   JoinColumnNamingStrategy joinColumnNamingStrategy,
									   AssociationTableNamingStrategy associationTableNamingStrategy,
									   ColumnNamingStrategy indexColumnNamingStrategy) {
		this.oneToManyRelation = oneToManyRelation;
		this.sourcePersister = sourcePersister;
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.persisterRegistry = persisterRegistry;
		
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.associationTableNamingStrategy = associationTableNamingStrategy;
		this.indexColumnNamingStrategy = indexColumnNamingStrategy;
	}
	
	public <T extends Table<T>> void configure(PersisterBuilderImpl<TRGT, TRGTID> targetPersisterBuilder) {
		Table targetTable = determineTargetTable(oneToManyRelation);
		ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister = targetPersisterBuilder
				.build(dialect, connectionConfiguration, persisterRegistry, targetTable);
		
		configure(targetPersister);
	}
	
	public CascadeConfigurationResult<SRC, TRGT> configureWithSelectIn2Phases(String tableAlias,
																			  ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
																			  FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
		PrimaryKey<?, SRCID> leftPrimaryKey = lookupSourcePrimaryKey(sourcePersister);
		
		RelationMode maintenanceMode = oneToManyRelation.getRelationMode();
		// selection is always present (else configuration is nonsense !)
		boolean orphanRemoval = maintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL;
		boolean writeAuthorized = maintenanceMode != RelationMode.READ_ONLY;
		
		OneToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C, ?> associationConfiguration = new OneToManyAssociationConfiguration<>(oneToManyRelation,
				sourcePersister, leftPrimaryKey,
				foreignKeyNamingStrategy, joinColumnNamingStrategy, indexColumnNamingStrategy,
				orphanRemoval, writeAuthorized);
		OneToManyConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, C, ?> configurer;
		if (oneToManyRelation.isOwnedByReverseSide()) {
			// case : reverse property is defined through one of the setter, getter or column on the reverse side
			if (maintenanceMode == RelationMode.ASSOCIATION_ONLY) {
				throw new MappingConfigurationException(RelationMode.ASSOCIATION_ONLY + " is only relevant with an association table");
			}
			configurer = new OneToManyWithMappedAssociationConfigurer<>(associationConfiguration, targetPersister, oneToManyRelation.isFetchSeparately());
		} else {
			configurer = new OneToManyWithAssociationTableConfigurer<>(associationConfiguration,
					targetPersister,
					oneToManyRelation.isFetchSeparately(),
					associationTableNamingStrategy,
					maintenanceMode == RelationMode.ASSOCIATION_ONLY,
					dialect,
					connectionConfiguration);
		}
		return configurer.configureWithSelectIn2Phases(tableAlias, firstPhaseCycleLoadListener);
	}
	
	void configure(ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		PrimaryKey<?, SRCID> leftPrimaryKey = lookupSourcePrimaryKey(sourcePersister);
		
		RelationMode maintenanceMode = oneToManyRelation.getRelationMode();
		// selection is always present (else configuration is nonsense !)
		boolean orphanRemoval = maintenanceMode == RelationMode.ALL_ORPHAN_REMOVAL;
		boolean writeAuthorized = maintenanceMode != RelationMode.READ_ONLY;
		
		OneToManyAssociationConfiguration<SRC, TRGT, SRCID, TRGTID, C, ?> associationConfiguration = new OneToManyAssociationConfiguration<>(oneToManyRelation,
				sourcePersister, leftPrimaryKey,
				foreignKeyNamingStrategy, joinColumnNamingStrategy, indexColumnNamingStrategy,
				orphanRemoval, writeAuthorized);
		OneToManyConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, C, ?> configurer;
		if (oneToManyRelation.isOwnedByReverseSide()) {
			// case : reverse property is defined through one of the setter, getter or column on the reverse side
			if (maintenanceMode == RelationMode.ASSOCIATION_ONLY) {
				throw new MappingConfigurationException(RelationMode.ASSOCIATION_ONLY + " is only relevant with an association table");
			}
			configurer = new OneToManyWithMappedAssociationConfigurer<>(associationConfiguration, targetPersister, oneToManyRelation.isFetchSeparately());
		} else {
			configurer = new OneToManyWithAssociationTableConfigurer<>(associationConfiguration,
					targetPersister,
					oneToManyRelation.isFetchSeparately(),
					associationTableNamingStrategy,
					maintenanceMode == RelationMode.ASSOCIATION_ONLY,
					dialect,
					connectionConfiguration);
		}
		configurer.configure();
	}
	
	private Table determineTargetTable(OneToManyRelation<SRC, TRGT, TRGTID, C> oneToManyRelation) {
		Table reverseTable = nullable(oneToManyRelation.getReverseColumn()).map(Column::getTable).get();
		Table indexingTable = oneToManyRelation instanceof OneToManyListRelation
				? nullable(((OneToManyListRelation<?, ?, ?, ?>) oneToManyRelation).getIndexingColumn()).map(Column::getTable).get()
				: null;
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
		return nullable(oneToManyRelation.getTargetTable()).elseSet(reverseTable).elseSet(indexingTable).get();
	}
	
	protected PrimaryKey<?, SRCID> lookupSourcePrimaryKey(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister) {
		return sourcePersister.getMapping().getTargetTable().getPrimaryKey();
	}
	
	/**
	 * Object invoked on row read
	 * @param <SRC>
	 * @param <TRGTID>
	 */
	@FunctionalInterface
	public interface FirstPhaseCycleLoadListener<SRC, TRGTID> {
		
		void onFirstPhaseRowRead(SRC src, TRGTID targetId);
		
	}
}
