package org.codefilarete.stalactite.engine.configurer;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.JoinTablePolymorphism;
import org.codefilarete.stalactite.engine.SubEntityMappingConfiguration;
import org.codefilarete.stalactite.engine.TableNamingStrategy;
import org.codefilarete.stalactite.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.runtime.JoinTablePolymorphismPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.EntityMerger.EntityMergerAdapter;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.Identification;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.codefilarete.stalactite.mapping.ClassMappingStrategy;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.tool.Nullable.nullable;

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
