package org.codefilarete.stalactite.engine.configurer.polymorphism;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.JoinTablePolymorphism;
import org.codefilarete.stalactite.engine.SubEntityMappingConfiguration;
import org.codefilarete.stalactite.engine.TableNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.BeanMapping;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.JoinTablePolymorphismPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.exception.NotImplementedException;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * @author Guillaume Mary
 */
class JoinTablePolymorphismBuilder<C, I, T extends Table<T>> extends AbstractPolymorphicPersisterBuilder<C, I, T> {
	
	private final JoinTablePolymorphism<C> joinTablePolymorphism;
	private final PrimaryKey<T, I> mainTablePrimaryKey;
	
	JoinTablePolymorphismBuilder(JoinTablePolymorphism<C> polymorphismPolicy,
								 AbstractIdentification<C, I> identification,
								 ConfiguredRelationalPersister<C, I> mainPersister,
								 ColumnBinderRegistry columnBinderRegistry,
								 ColumnNameProvider columnNameProvider,
								 TableNamingStrategy tableNamingStrategy,
								 ColumnNamingStrategy columnNamingStrategy,
								 ForeignKeyNamingStrategy foreignKeyNamingStrategy,
								 ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
								 JoinColumnNamingStrategy joinColumnNamingStrategy,
								 ColumnNamingStrategy indexColumnNamingStrategy,
								 AssociationTableNamingStrategy associationTableNamingStrategy) {
		super(polymorphismPolicy, identification, mainPersister, columnBinderRegistry, columnNameProvider, columnNamingStrategy, foreignKeyNamingStrategy,
				elementCollectionTableNamingStrategy, joinColumnNamingStrategy, indexColumnNamingStrategy, associationTableNamingStrategy, tableNamingStrategy);
		this.joinTablePolymorphism = polymorphismPolicy;
		this.mainTablePrimaryKey = (PrimaryKey<T, I>) this.mainPersister.getMapping().getTargetTable().getPrimaryKey();
	}
	
	@Override
	public ConfiguredRelationalPersister<C, I> build(Dialect dialect, ConnectionConfiguration connectionConfiguration, PersisterRegistry persisterRegistry) {
		Map<Class<C>, ConfiguredRelationalPersister<C, I>> persisterPerSubclass = collectSubClassPersister(dialect, connectionConfiguration);
		
		registerCascades(persisterPerSubclass, dialect, connectionConfiguration, persisterRegistry);
		
		return new JoinTablePolymorphismPersister<>(
				mainPersister, persisterPerSubclass, connectionConfiguration.getConnectionProvider(),
				dialect);
	}
	
	private <D extends C> Map<Class<D>, ConfiguredRelationalPersister<D, I>> collectSubClassPersister(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		Map<Class<D>, ConfiguredRelationalPersister<D, I>> persisterPerSubclass = new HashMap<>();
		
		BeanMappingBuilder<D, ?> beanMappingBuilder = new BeanMappingBuilder<>();
		for (SubEntityMappingConfiguration<D> subConfiguration : ((Set<SubEntityMappingConfiguration<D>>) (Set) joinTablePolymorphism.getSubClasses())) {
			ConfiguredRelationalPersister<D, I> subclassPersister = buildSubclassPersister(dialect, connectionConfiguration, beanMappingBuilder, subConfiguration);
			// Adding join with parent table to select
			subclassPersister.getEntityJoinTree().addMergeJoin(EntityJoinTree.ROOT_STRATEGY_NAME,
					new EntityMergerAdapter<C, T>(mainPersister.getMapping()),
					subclassPersister.getMapping().getTargetTable().getPrimaryKey(),
					this.mainTablePrimaryKey);
			persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
		}
		return persisterPerSubclass;
	}
	
	private <D, SUBT extends Table<SUBT>> ConfiguredRelationalPersister<D, I> buildSubclassPersister(Dialect dialect,
																										   ConnectionConfiguration connectionConfiguration,
																										   BeanMappingBuilder<D, SUBT> beanMappingBuilder,
																										   SubEntityMappingConfiguration<D> subConfiguration) {
		// first we'll use table of columns defined in embedded override
		// then the one defined by inheritance
		// if both are null we'll create a new one
		Table tableDefinedByColumnOverride = BeanMappingBuilder.giveTargetTable(subConfiguration.getPropertiesMapping());
		Table tableDefinedByInheritanceConfiguration = joinTablePolymorphism.giveTable(subConfiguration);
		
		assertNullOrEqual(tableDefinedByColumnOverride, tableDefinedByInheritanceConfiguration);
		
		SUBT subTable = (SUBT) nullable(tableDefinedByColumnOverride)
				.elseSet(tableDefinedByInheritanceConfiguration)
				.getOr(() -> new Table<>(tableNamingStrategy.giveName(subConfiguration.getEntityType())));
		
		BeanMapping<D, SUBT> beanMapping = beanMappingBuilder.build(subConfiguration.getPropertiesMapping(), subTable,
				this.columnBinderRegistry, this.columnNameProvider);
		Map<ReversibleAccessor<D, Object>, Column<SUBT, Object>> subEntityPropertiesMapping = beanMapping.getMapping();
		Map<ReversibleAccessor<D, Object>, Column<SUBT, Object>> subEntityReadonlyPropertiesMapping = beanMapping.getReadonlyMapping();
		addPrimarykey(subTable);
		addForeignKey(subTable);
		Mapping<D, SUBT> subEntityMapping = new Mapping<>(subConfiguration, subTable, subEntityPropertiesMapping, subEntityReadonlyPropertiesMapping, false);
		addIdentificationToMapping(identification, subEntityMapping);
		ClassMapping<D, I, SUBT> classMappingStrategy = PersisterBuilderImpl.createClassMappingStrategy(
				false,
				subTable,
				subEntityPropertiesMapping,
				subEntityReadonlyPropertiesMapping,
				new ValueAccessPointSet<>(),    // TODO: implement properties set by constructor feature in joined-tables polymorphism
				(AbstractIdentification<D, I>) identification,
				subConfiguration.getPropertiesMapping().getBeanType(),
				null);
		
		// NB: persisters are not registered into PersistenceContext because it may break implicit polymorphism principle (persisters are then
		// available by PersistenceContext.getPersister(..)) and it is not sure that they are perfect ones (all their features should be tested)
		return new SimpleRelationalEntityPersister<>(classMappingStrategy, dialect, connectionConfiguration);
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
		PersisterBuilderImpl.propagatePrimaryKey(this.mainTablePrimaryKey, Arrays.asSet(table));
	}
	
	private void addForeignKey(Table table) {
		PersisterBuilderImpl.applyForeignKeys(this.mainTablePrimaryKey, this.foreignKeyNamingStrategy, Arrays.asSet(table));
	}
	
	private void addIdentificationToMapping(AbstractIdentification<C, I> identification, Mapping mapping) {
		PersisterBuilderImpl.addIdentificationToMapping(identification, Arrays.asSet(mapping));
	}
}
