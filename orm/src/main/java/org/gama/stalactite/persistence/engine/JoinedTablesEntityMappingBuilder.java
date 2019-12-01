package org.gama.stalactite.persistence.engine;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.gama.lang.collection.Iterables;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration.InheritanceConfiguration;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeInsertSupport;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.UpdateByIdListener;
import org.gama.stalactite.persistence.id.assembly.SimpleIdentifierAssembler;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdAccessor;
import org.gama.stalactite.persistence.mapping.SimpleIdMappingStrategy;
import org.gama.stalactite.persistence.mapping.SinglePropertyIdAccessor;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.Nullable.nullable;

/**
 * Class aimed at building a persister for joined tables inheritance
 * 
 * @author Guillaume Mary
 * @see #build(PersistenceContext, Table) 
 */
public class JoinedTablesEntityMappingBuilder<C, I> extends AbstractEntityMappingBuilder<C, I> {
	
	/**
	 * Constructor with madatory parameters
	 * 
	 * @param entityMappingConfiguration the {@link EntityMappingConfiguration} which has a parent configuration
	 * @param methodSpy capturer of {@link java.lang.reflect.Method}s behind method references, passed as argument to benefit from its cache
	 */
	public JoinedTablesEntityMappingBuilder(EntityMappingConfiguration<C, I> entityMappingConfiguration, MethodReferenceCapturer methodSpy) {
		super(entityMappingConfiguration, methodSpy);
	}
	
	@Override
	protected <T extends Table> JoinedTablesPersister<C, I, T> doBuild(PersistenceContext persistenceContext, T childClassTargetTable) {
		// We're going to create 2 JoinedTablesPersister (one for parent strategy, one for child strategy) then we will join them alltogether
		// on primary key, so select will result in a join on those 2 strategies.
		// OneToOne and OneToMany will be managed by their respective JoinedTablesPersister (actually done by Persister)
		
		JoinedTablesPersister<? super C, I, Table> superPersister = buildSuperPersister(persistenceContext);
		
		IEntityMappingStrategy<? super C, I, ?> parentMappingStrategy = superPersister.getMappingStrategy();
		IReversibleAccessor<C, I> identifierAccessor = giveIdentifierAccessor(parentMappingStrategy);
		
		ClassMappingStrategy<C, I, T> childClassMappingStrategy = buildChildClassMappingStrategy(
				// identifier type can also be deduced from identifierAccessor with Accessor.giveReturnType(..) but getting it from insertion manager
				// is more straightforward because it doesn't need any algorithm to discover accessor type (see giveReturnType(..))
				parentMappingStrategy.getIdMappingStrategy().getIdentifierInsertionManager().getIdentifierType(),
				identifierAccessor,
				childClassTargetTable,
				superPersister.getMainTable(),
				persistenceContext.getDialect());
		
		JoinedTablesPersister<C, I, T> result = new EntityMappingBuilder<>(configurationSupport, methodSpy).createPersister(persistenceContext, childClassMappingStrategy);
		// adding join on parent table
		Column subclassPK = Iterables.first((Set<? extends Column<?, Object>>) childClassTargetTable.getPrimaryKey().getColumns());
		Column superclassPK = Iterables.first((Set<? extends Column<?, Object>>) superPersister.getMainTable().getPrimaryKey().getColumns());
		result.getJoinedStrategiesSelectExecutor().addRelation(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, parentMappingStrategy, (target, input) -> {
			// applying values from inherited bean (input) to subclass one (target)
			for (IReversibleAccessor columnFieldEntry : parentMappingStrategy.getPropertyToColumn().keySet()) {
				columnFieldEntry.toMutator().set(target, columnFieldEntry.get(input));
			}
		}, subclassPK, superclassPK, false);
		
		addCascadesBetweenChildAndParentTable(result.getPersisterListener(), superPersister);
		
		return result;
	}
	
	private <T extends Table> ClassMappingStrategy<C, I, T> buildChildClassMappingStrategy(Class<I> identifierType,
																						   IReversibleAccessor<C, I> identifierAccessor,
																						   T childTargetTable,
																						   Table parentTargetTable,
																						   Dialect dialect) {
		EmbeddableMappingBuilder<C> builder = new EmbeddableMappingBuilder<>(configurationSupport.getPropertiesMapping(), columnNameProvider);
		Map<IReversibleAccessor, Column> childClassColumnMapping = builder.build(dialect, childTargetTable);
		// getting primary key from parent table and applying it to child table
		Set<Column<Table, Object>> primaryKey = parentTargetTable.getPrimaryKey().getColumns();
		for (Column column : primaryKey) {
			childTargetTable.addColumn(column.getName(), column.getJavaType()).primaryKey();
		}
		
		// for now only single column primary key are really supported
		Set<? extends Column<?, Object>> columns = childTargetTable.getPrimaryKey().getColumns();
		Column<?, Object> singlePrimaryKey = Iterables.first(columns);
		childClassColumnMapping.put(identifierAccessor, singlePrimaryKey);
		
		// Child class insertion manager is always an "Already assigned" one because parent manages it for her
		IdentifierInsertionManager<C, I> identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(identifierType);
		
		SimpleIdMappingStrategy<C, I> simpleIdMappingStrategy = new SimpleIdMappingStrategy<>(identifierAccessor, identifierInsertionManager,
				new SimpleIdentifierAssembler<>(singlePrimaryKey));
		
		return new ClassMappingStrategy<C, I, T>(configurationSupport.getEntityType(),
				childTargetTable, (Map) childClassColumnMapping, simpleIdMappingStrategy, configurationSupport.getEntityFactory());
	}
	
	private void addCascadesBetweenChildAndParentTable(PersisterListener<C, I> persisterListener, Persister<? super C, I, Table> superPersister) {
		// Before insert of child we must insert parent
		persisterListener.addInsertListener(new BeforeInsertSupport<>(superPersister::insert, Function.identity()));
		
		// On child update, parent must be updated too, no constraint on order for this, after is arbitrarly choosen
		persisterListener.addUpdateListener(new AfterUpdateSupport<>(superPersister::update, Function.identity()));
		// idem for updateById
		persisterListener.addUpdateByIdListener(new UpdateByIdListener<C>() {
			@Override
			public void afterUpdateById(Iterable<C> entities) {
				superPersister.updateById((Iterable) entities);
			}
		});
		
		// On child deletion, parent must be deleted first
		// this weird cast is due to <? super C> ...
		Consumer<Iterable<C>> superEntitiesDeletor = (Consumer) (Consumer<Iterable>) superPersister::delete;
		persisterListener.addDeleteListener(new BeforeDeleteSupport<>(superEntitiesDeletor, Function.identity()));
		// idem for deleteById
		Consumer<Iterable<C>> superEntitiesDeletorById = (Consumer) (Consumer<Iterable>) superPersister::deleteById;
		persisterListener.addDeleteByIdListener(new BeforeDeleteByIdSupport<>(superEntitiesDeletorById, Function.identity()));
	}
	
	private IReversibleAccessor<C, I> giveIdentifierAccessor(IEntityMappingStrategy<? super C, I, ?> parentMappingStrategy) {
		IdAccessor<? super C, ?> idAccessor = parentMappingStrategy.getIdMappingStrategy().getIdAccessor();
		if (!(idAccessor instanceof SinglePropertyIdAccessor)) {
			throw new NotYetSupportedOperationException();
		}
		return ((SinglePropertyIdAccessor<C, I>) idAccessor).getIdAccessor();
	}
	
	private JoinedTablesPersister<? super C, I, Table> buildSuperPersister(PersistenceContext persistenceContext) {
		InheritanceConfiguration<? super C, I> inheritanceConfiguration = configurationSupport.getInheritanceConfiguration();
		EntityMappingBuilder<? super C, I> inheritanceMappingBuilder = new EntityMappingBuilder<>(inheritanceConfiguration.getConfiguration(), methodSpy);
		Table inheritanceTable = nullable(inheritanceConfiguration.getTable()).getOr(
				() -> new Table(configurationSupport.getTableNamingStrategy().giveName(inheritanceConfiguration.getConfiguration().getEntityType())));
		return inheritanceMappingBuilder.build(persistenceContext, inheritanceTable);
	}
}
