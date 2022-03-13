package org.codefilarete.stalactite.persistence.engine.configurer;

import java.util.Map;

import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.persistence.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.persistence.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.persistence.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.persistence.engine.PersisterRegistry;
import org.codefilarete.stalactite.persistence.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.persistence.engine.PolymorphismPolicy.JoinTablePolymorphism;
import org.codefilarete.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.persistence.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.codefilarete.stalactite.persistence.engine.TableNamingStrategy;
import org.codefilarete.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.codefilarete.stalactite.persistence.engine.configurer.PersisterBuilderImpl.Identification;
import org.codefilarete.stalactite.persistence.engine.configurer.PersisterBuilderImpl.PolymorphismBuilder;
import org.codefilarete.stalactite.persistence.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.persistence.engine.runtime.PersisterListenerWrapper;
import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.persistence.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;

/**
 * Builder of polymorphic persisters. Handles {@link PolymorphismPolicy} subtypes, as such, it is the main entry point for polymorphic persisters :
 * it will invoke {@link JoinTablePolymorphismBuilder}, {@link SingleTablePolymorphismBuilder} or {@link TablePerClassPolymorphismBuilder}
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
	private final Map<ReversibleAccessor, Column> mainMapping;
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
								 Map<ReversibleAccessor, Column> mainMapping,
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
		} else if (polymorphismPolicy instanceof PolymorphismPolicy.JoinTablePolymorphism) {
			polymorphismBuilder = new JoinTablePolymorphismBuilder<>((JoinTablePolymorphism<C>) polymorphismPolicy,
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
