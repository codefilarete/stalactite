package org.gama.stalactite.persistence.engine.configurer;

import java.util.HashMap;
import java.util.Map;

import org.gama.lang.Reflections;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.exception.NotImplementedException;
import org.gama.reflection.ReversibleAccessor;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.engine.AssociationTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ElementCollectionTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.PersisterRegistry;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.JoinTablePolymorphism;
import org.gama.stalactite.persistence.engine.SubEntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.TableNamingStrategy;
import org.gama.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.Identification;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.gama.stalactite.persistence.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.JoinTablePolymorphismPersister;
import org.gama.stalactite.persistence.engine.runtime.SimpleRelationalEntityPersister;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityMerger.EntityMergerAdapter;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ConnectionConfiguration;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.PrimaryKey;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.Nullable.nullable;

/**
 * @author Guillaume Mary
 */
class JoinTablePolymorphismBuilder<C, I, T extends Table> extends AbstractPolymorphicPersisterBuilder<C, I, T> {
	
	private final JoinTablePolymorphism<C> joinTablePolymorphism;
	private final PrimaryKey mainTablePrimaryKey;
	
	JoinTablePolymorphismBuilder(JoinTablePolymorphism<C> polymorphismPolicy,
								 Identification identification,
								 EntityConfiguredJoinedTablesPersister<C, I> mainPersister,
								 ColumnBinderRegistry columnBinderRegistry,
								 ColumnNameProvider columnNameProvider,
								 TableNamingStrategy tableNamingStrategy,
								 ColumnNamingStrategy columnNamingStrategy,
								 ForeignKeyNamingStrategy foreignKeyNamingStrategy,
								 ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
								 ColumnNamingStrategy joinColumnNamingStrategy,
								 ColumnNamingStrategy indexColumnNamingStrategy,
								 AssociationTableNamingStrategy associationTableNamingStrategy) {
		super(polymorphismPolicy, identification, mainPersister, columnBinderRegistry, columnNameProvider, columnNamingStrategy, foreignKeyNamingStrategy,
				elementCollectionTableNamingStrategy, joinColumnNamingStrategy, indexColumnNamingStrategy, associationTableNamingStrategy, tableNamingStrategy);
		this.joinTablePolymorphism = polymorphismPolicy;
		this.mainTablePrimaryKey = this.mainPersister.getMappingStrategy().getTargetTable().getPrimaryKey();
	}
	
	@Override
	public EntityConfiguredJoinedTablesPersister<C, I> build(Dialect dialect, ConnectionConfiguration connectionConfiguration, PersisterRegistry persisterRegistry) {
		Map<Class<? extends C>, EntityConfiguredJoinedTablesPersister<C, I>> persisterPerSubclass = new HashMap<>();
		
		BeanMappingBuilder beanMappingBuilder = new BeanMappingBuilder();
		for (SubEntityMappingConfiguration<? extends C> subConfiguration : joinTablePolymorphism.getSubClasses()) {
			EntityConfiguredJoinedTablesPersister<? extends C, I> subclassPersister;
			
			// first we'll use table of columns defined in embedded override
			// then the one defined by inheritance
			// if both are null we'll create a new one
			Table tableDefinedByColumnOverride = BeanMappingBuilder.giveTargetTable(subConfiguration.getPropertiesMapping());
			Table tableDefinedByInheritanceConfiguration = joinTablePolymorphism.giveTable(subConfiguration);
			
			assertNullOrEqual(tableDefinedByColumnOverride, tableDefinedByInheritanceConfiguration);
			
			Table subTable = nullable(tableDefinedByColumnOverride)
					.elseSet(tableDefinedByInheritanceConfiguration)
					.getOr(() -> new Table(tableNamingStrategy.giveName(subConfiguration.getEntityType())));
			
			Map<ReversibleAccessor, Column> subEntityPropertiesMapping = beanMappingBuilder.build(subConfiguration.getPropertiesMapping(), subTable,
					this.columnBinderRegistry, this.columnNameProvider);
			addPrimarykey(subTable);
			addForeignKey(subTable);
			Mapping subEntityMapping = new Mapping(subConfiguration, subTable, subEntityPropertiesMapping, false);
			addIdentificationToMapping(identification, subEntityMapping);
			ClassMappingStrategy<? extends C, I, Table> classMappingStrategy = PersisterBuilderImpl.createClassMappingStrategy(
					false,
					subTable,
					subEntityPropertiesMapping,
					new ValueAccessPointSet(),	// TODO: implement properties set by constructor feature in joined-tables polymorphism
					identification,
					subConfiguration.getPropertiesMapping().getBeanType(),
					null);
			
			// NB: persisters are not registered into PersistenceContext because it may break implicit polymorphism principle (persisters are then
			// available by PersistenceContext.getPersister(..)) and it is not sure that they are perfect ones (all their features should be tested)
			subclassPersister = new SimpleRelationalEntityPersister(classMappingStrategy, dialect, connectionConfiguration);
			
			// Adding join with parent table to select
			Column subEntityPrimaryKey = (Column) Iterables.first(subclassPersister.getMappingStrategy().getTargetTable().getPrimaryKey().getColumns());
			Column entityPrimaryKey = (Column) Iterables.first(this.mainTablePrimaryKey.getColumns());
			subclassPersister.getEntityJoinTree().addMergeJoin(EntityJoinTree.ROOT_STRATEGY_NAME,
					new EntityMergerAdapter<C, Table>(mainPersister.getMappingStrategy()),
					subEntityPrimaryKey, entityPrimaryKey);
			
			persisterPerSubclass.put(subConfiguration.getEntityType(), (EntityConfiguredJoinedTablesPersister<C, I>) subclassPersister);
		}
		
		registerCascades(persisterPerSubclass, dialect, connectionConfiguration, persisterRegistry);
		
		JoinTablePolymorphismPersister<C, I> surrogate = new JoinTablePolymorphismPersister<>(
				mainPersister, persisterPerSubclass, connectionConfiguration.getConnectionProvider(),
				dialect);
		return surrogate;
	}
	
	@Override
	protected void assertSubPolymorphismIsSupported(PolymorphismPolicy<? extends C> subPolymorphismPolicy) {
		// Everything else than joined-tables and single-table is not implemented (meaning table-per-class)
		// Written as a negative condition to explicitly say what we support
		if (!(subPolymorphismPolicy instanceof PolymorphismPolicy.JoinTablePolymorphism
				|| subPolymorphismPolicy instanceof PolymorphismPolicy.SingleTablePolymorphism)) {
			throw new NotImplementedException("Combining joined-tables polymorphism policy with " + Reflections.toString(subPolymorphismPolicy.getClass()));
		}
	}
	
	private void addPrimarykey(Table table) {
		PersisterBuilderImpl.propagatePrimarykey(this.mainTablePrimaryKey, Arrays.asSet(table));
	}
	
	private void addForeignKey(Table table) {
		PersisterBuilderImpl.applyForeignKeys(this.mainTablePrimaryKey, this.foreignKeyNamingStrategy, Arrays.asSet(table));
	}
	
	private void addIdentificationToMapping(Identification identification, Mapping mapping) {
		PersisterBuilderImpl.addIdentificationToMapping(identification, Arrays.asSet(mapping));
	}
}
