package org.gama.stalactite.persistence.engine.configurer;

import java.util.Map;

import org.gama.lang.exception.NotImplementedException;
import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.engine.AssociationTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ElementCollectionTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.PersisterRegistry;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.JoinedTablesPolymorphism;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.gama.stalactite.persistence.engine.TableNamingStrategy;
import org.gama.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.Identification;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.PolymorphismBuilder;
import org.gama.stalactite.persistence.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.PersisterListenerWrapper;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ConnectionConfiguration;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Builder of polymorphic persisters. Handles {@link PolymorphismPolicy} subtypes, as such, it is the main entry point for polymorphic persisters :
 * it will invoke {@link JoinedTablesPolymorphismBuilder}, {@link SingleTablePolymorphismBuilder} or {@link TablePerClassPolymorphismBuilder}
 * accoring to policy. Hence those builders are not expected to be invoked directly outside of this class.
 * 
 * @author Guillaume Mary
 */
class PolymorphismPersisterBuilder<C, I, T extends Table> implements PolymorphismBuilder<C, I, T> {
	
	private final PolymorphismPolicy<C> polymorphismPolicy;
	private final EntityConfiguredJoinedTablesPersister<C, I> mainPersister;
	private final Identification identification;
	private final ColumnBinderRegistry columnBinderRegistry;
	private final ColumnNameProvider columnNameProvider;
	
	private final ColumnNamingStrategy columnNamingStrategy;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy;
	private final ColumnNamingStrategy joinColumnNamingStrategy;
	private final ColumnNamingStrategy indexColumnNamingStrategy;
	private final AssociationTableNamingStrategy associationTableNamingStrategy;
	private final Map<IReversibleAccessor, Column> mainMapping;
	private final TableNamingStrategy tableNamingStrategy;
	
	PolymorphismPersisterBuilder(PolymorphismPolicy<C> polymorphismPolicy,
								 Identification identification,
								 EntityConfiguredJoinedTablesPersister<C, I> mainPersister,
								 ColumnBinderRegistry columnBinderRegistry,
								 ColumnNameProvider columnNameProvider,
								 ColumnNamingStrategy columnNamingStrategy,
								 ForeignKeyNamingStrategy foreignKeyNamingStrategy,
								 ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
								 ColumnNamingStrategy joinColumnNamingStrategy,
								 ColumnNamingStrategy indexColumnNamingStrategy,
								 AssociationTableNamingStrategy associationTableNamingStrategy,
								 Map<IReversibleAccessor, Column> mainMapping,
								 TableNamingStrategy tableNamingStrategy) {
		this.polymorphismPolicy = polymorphismPolicy;
		this.identification = identification;
		this.mainPersister = mainPersister;
		this.columnBinderRegistry = columnBinderRegistry;
		this.columnNameProvider = columnNameProvider;
		this.columnNamingStrategy = columnNamingStrategy;
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		this.elementCollectionTableNamingStrategy = elementCollectionTableNamingStrategy;
		this.joinColumnNamingStrategy = joinColumnNamingStrategy;
		this.indexColumnNamingStrategy = indexColumnNamingStrategy;
		this.associationTableNamingStrategy = associationTableNamingStrategy;
		this.mainMapping = mainMapping;
		this.tableNamingStrategy = tableNamingStrategy;
	}
	
	@Override
	public EntityConfiguredJoinedTablesPersister<C, I> build(Dialect dialect, ConnectionConfiguration connectionConfiguration, PersisterRegistry persisterRegistry) {
		PolymorphismBuilder<C, I, T> polymorphismBuilder;
		if (polymorphismPolicy instanceof PolymorphismPolicy.SingleTablePolymorphism) {
			polymorphismBuilder = new SingleTablePolymorphismBuilder<>((SingleTablePolymorphism<C, ?>) polymorphismPolicy,
					this.identification, this.mainPersister, this.mainMapping, this.columnBinderRegistry, this.columnNameProvider, this.tableNamingStrategy,
					this.columnNamingStrategy, this.foreignKeyNamingStrategy, this.elementCollectionTableNamingStrategy,
					this.joinColumnNamingStrategy, this.indexColumnNamingStrategy,
					this.associationTableNamingStrategy);
		} else if (polymorphismPolicy instanceof PolymorphismPolicy.TablePerClassPolymorphism) {
			polymorphismBuilder = new TablePerClassPolymorphismBuilder<>((TablePerClassPolymorphism<C>) polymorphismPolicy,
					this.identification, this.mainPersister, this.mainMapping, this.columnBinderRegistry, this.columnNameProvider, this.tableNamingStrategy,
					this.columnNamingStrategy, this.foreignKeyNamingStrategy, this.elementCollectionTableNamingStrategy,
					this.joinColumnNamingStrategy, this.indexColumnNamingStrategy,
					this.associationTableNamingStrategy);
		} else if (polymorphismPolicy instanceof PolymorphismPolicy.JoinedTablesPolymorphism) {
			polymorphismBuilder = new JoinedTablesPolymorphismBuilder<>((JoinedTablesPolymorphism<C>) polymorphismPolicy,
					this.identification, this.mainPersister, this.columnBinderRegistry, this.columnNameProvider, this.tableNamingStrategy,
					this.columnNamingStrategy, this.foreignKeyNamingStrategy, this.elementCollectionTableNamingStrategy,
					this.joinColumnNamingStrategy, this.indexColumnNamingStrategy,
					this.associationTableNamingStrategy);
		} else {
			// this exception is more to satisfy Sonar than for real case
			throw new NotImplementedException("Given policy is not implemented : " + polymorphismPolicy);
		}
		EntityConfiguredJoinedTablesPersister<C, I> result = polymorphismBuilder.build(dialect, connectionConfiguration, persisterRegistry);
		result = new PersisterListenerWrapper<>(result);
		// We transfert listeners so that all actions are made in the same "event listener context" : all listeners are agregated in a top level one.
		// Made in particular for already-assigned mark-as-persisted mecanism and relation cascade triggering.
		mainPersister.getPersisterListener().moveTo(result.getPersisterListener());
		
		return result;
	}
}
