package org.gama.stalactite.persistence.engine.configurer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.engine.IEntityConfiguredPersister;
import org.gama.stalactite.persistence.engine.IUpdateExecutor;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.JoinedTablesPolymorphism;
import org.gama.stalactite.persistence.engine.SubEntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.TableNamingStrategy;
import org.gama.stalactite.persistence.engine.UpdateExecutor;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.Identification;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.PolymorphismBuilder;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.Nullable.nullable;

/**
 * @author Guillaume Mary
 */
abstract class JoinedTablesPolymorphismBuilder<C, I, T extends Table> implements PolymorphismBuilder<C, I, T> {
	
	private final JoinedTablesPolymorphism<C, I> polymorphismPolicy;
	private final JoinedTablesPersister<C, I, T> mainPersister;
	private final ColumnBinderRegistry columnBinderRegistry;
	private final ColumnNameProvider columnNameProvider;
	private final Identification identification;
	private final TableNamingStrategy tableNamingStrategy;
	private final Set<Table> tables = new HashSet<>();
	private Map<Class<? extends C>, IUpdateExecutor<C>> subEntitiesUpdaters;
	
	JoinedTablesPolymorphismBuilder(JoinedTablesPolymorphism<C, I> polymorphismPolicy,
									Identification identification,
									JoinedTablesPersister<C, I, T> mainPersister,
									ColumnBinderRegistry columnBinderRegistry,
									ColumnNameProvider columnNameProvider,
									TableNamingStrategy tableNamingStrategy) {
		this.polymorphismPolicy = polymorphismPolicy;
		this.identification = identification;
		this.mainPersister = mainPersister;
		this.columnBinderRegistry = columnBinderRegistry;
		this.columnNameProvider = columnNameProvider;
		this.tableNamingStrategy = tableNamingStrategy;
	}
	
	private Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> buildSubEntitiesPersisters(PersistenceContext persistenceContext) {
		Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass = new HashMap<>();
		
		BeanMappingBuilder beanMappingBuilder = new BeanMappingBuilder();
		
		subEntitiesUpdaters = new HashMap<>();
		for (SubEntityMappingConfiguration<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
			Table subTable = nullable(polymorphismPolicy.giveTable(subConfiguration))
					.getOr(() -> new Table(tableNamingStrategy.giveName(subConfiguration.getEntityType())));
			tables.add(subTable);
			Map<IReversibleAccessor, Column> subEntityPropertiesMapping = beanMappingBuilder.build(subConfiguration.getPropertiesMapping(), subTable,
					this.columnBinderRegistry, this.columnNameProvider);
			addPrimarykey(identification, subTable);
			addForeignKey(identification, subTable);
			Mapping subEntityMapping = new Mapping(subConfiguration, subTable, subEntityPropertiesMapping, false);
			addIdentificationToMapping(identification, subEntityMapping);
			ClassMappingStrategy<? extends C, I, Table> classMappingStrategy = PersisterBuilderImpl.createClassMappingStrategy(
					false,
					subTable,
					subEntityPropertiesMapping,
					identification,
					// TODO: no generated keys handler for now, should be taken on main strategy or brought by identification
					null,
					subConfiguration.getPropertiesMapping().getBeanType());
			
			JoinedTablesPersister subclassPersister = new JoinedTablesPersister(persistenceContext, classMappingStrategy);
			persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
			
			// Adding join with parent table to select
			Column subEntityPrimaryKey = (Column) Iterables.first(subTable.getPrimaryKey().getColumns());
			Column entityPrimaryKey = (Column) Iterables.first(this.mainPersister.getMainTable().getPrimaryKey().getColumns());
			subclassPersister.getJoinedStrategiesSelectExecutor().addComplementaryJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME,
					this.mainPersister.getMappingStrategy(), subEntityPrimaryKey, entityPrimaryKey);
			
			// creating dedicated instance updater because default one only takes its declared properties into account
			subEntitiesUpdaters.put(subConfiguration.getEntityType(), new IUpdateExecutor<C>() {
				
				/**
				 * Overriden to transfer {@link #updateById(Iterable)} to main persister and sub persisters
				 * @param entities iterable of entities
				 * @return
				 */
				@Override
				public int updateById(Iterable<C> entities) {
					int mainAffectedRowCount = mainPersister.updateById(entities);
					int subAffectedRowCount = subclassPersister.updateById(entities);
					// since we update without any versioning info (only by id) we return effective updated row count
					return mainAffectedRowCount + subAffectedRowCount;
				}
				
				/**
				 * Overriden to take parent properties into account, because default sub entity persister takes only their declared properties
				 * in difference computation and SQL update build.
				 * 
				 * @param differencesIterable an iteration of modifed vs unmodified instances
				 * @param allColumnsStatement
				 * @return number of row updated
				 */
				@Override
				public int update(Iterable<? extends Duo<? extends C, ? extends C>> differencesIterable, boolean allColumnsStatement) {
					List<UpdatePayload<C, Table>> updatePayloads = new ArrayList<>();
					differencesIterable.forEach(duo ->
							updatePayloads.add(new UpdatePayload<>(duo, (Map) mainPersister.getMappingStrategy().getUpdateValues(duo.getLeft(),
							duo.getRight(), allColumnsStatement)))
					);
					int rowCount;
					if (Iterables.isEmpty(updatePayloads)) {
						// nothing to update => we return immediatly without any call to listeners
						rowCount = 0;
					} else {
						rowCount = ((UpdateExecutor) mainPersister.getUpdateExecutor()).updateDifferences(updatePayloads, allColumnsStatement);
					}
					
					updatePayloads.clear();
					differencesIterable.forEach(duo ->
						updatePayloads.add(new UpdatePayload<>(duo, subclassPersister.getMappingStrategy().getUpdateValues(duo.getLeft(),
								duo.getRight(), allColumnsStatement)))
					);
					if (!Iterables.isEmpty(updatePayloads)) {
						int subEntityRowCount = subclassPersister.getUpdateExecutor().updateDifferences(updatePayloads, allColumnsStatement);
						// RowCount is either 0 or number of rows updated. In the first case result might be number of rows updated by sub entities.
						// In second case we don't need to change it because sub entities updated row count will also be either 0 or total entities count  
						if (rowCount == 0) {
							rowCount = subEntityRowCount;
						}
					}
					
					return rowCount;
				}
			});
			
			
		}
		
		return persisterPerSubclass;
	}
	
	abstract void addPrimarykey(Identification identification, Table table);
	
	abstract void addForeignKey(Identification identification, Table table);
	
	abstract void addIdentificationToMapping(Identification identification, Mapping mapping);
	
	@Override
	public IEntityConfiguredPersister<C, I> build(PersistenceContext persistenceContext) {
		Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> subEntitiesPersisters = buildSubEntitiesPersisters(persistenceContext);
		// NB: persisters are not registered into PersistenceContext because it may break implicit polymorphism principle (persisters are then
		// available by PersistenceContext.getPersister(..)) and it is one sure that they are perfect ones (all their features should be tested)
		return new JoinedTablesPolymorphicPersister(mainPersister, subEntitiesPersisters,
				subEntitiesUpdaters,
				persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
	}
	
}
