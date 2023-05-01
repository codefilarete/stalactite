package org.codefilarete.stalactite.engine.configurer.polymorphism;

import java.util.Map;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.JoinTablePolymorphism;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.codefilarete.stalactite.engine.TableNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.Identification;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.PersisterListenerWrapper;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.exception.NotImplementedException;

/**
 * Builder of polymorphic persisters. Handles {@link PolymorphismPolicy} subtypes, as such, it is the main entry point for polymorphic persisters :
 * it will invoke {@link JoinTablePolymorphismBuilder}, {@link SingleTablePolymorphismBuilder} or {@link TablePerClassPolymorphismBuilder}
 * according to policy. Hence, those builders are not expected to be invoked directly outside this class.
 * 
 * @author Guillaume Mary
 */
public class PolymorphismPersisterBuilder<C, I, T extends Table> implements PolymorphismBuilder<C, I, T> {
	
	private final PolymorphismPolicy<C> polymorphismPolicy;
	private final ConfiguredRelationalPersister<C, I> mainPersister;
	private final Identification<C, I> identification;
	private final ColumnBinderRegistry columnBinderRegistry;
	private final ColumnNameProvider columnNameProvider;
	
	private final ColumnNamingStrategy columnNamingStrategy;
	private final ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private final ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy;
	private final JoinColumnNamingStrategy joinColumnNamingStrategy;
	private final ColumnNamingStrategy indexColumnNamingStrategy;
	private final AssociationTableNamingStrategy associationTableNamingStrategy;
	private final Map<ReversibleAccessor<C, Object>, Column<T, Object>> mainMapping;
	private final TableNamingStrategy tableNamingStrategy;
	
	public PolymorphismPersisterBuilder(PolymorphismPolicy<C> polymorphismPolicy,
										Identification<C, I> identification,
										ConfiguredRelationalPersister<C, I> mainPersister,
										ColumnBinderRegistry columnBinderRegistry,
										ColumnNameProvider columnNameProvider,
										ColumnNamingStrategy columnNamingStrategy,
										ForeignKeyNamingStrategy foreignKeyNamingStrategy,
										ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
										JoinColumnNamingStrategy joinColumnNamingStrategy,
										ColumnNamingStrategy indexColumnNamingStrategy,
										AssociationTableNamingStrategy associationTableNamingStrategy,
										Map<? extends ReversibleAccessor<C, Object>, Column<T, Object>> mainMapping,
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
		this.mainMapping = (Map<ReversibleAccessor<C, Object>, Column<T, Object>>) mainMapping;
		this.tableNamingStrategy = tableNamingStrategy;
	}
	
	@Override
	public ConfiguredRelationalPersister<C, I> build(Dialect dialect, ConnectionConfiguration connectionConfiguration, PersisterRegistry persisterRegistry) {
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
		ConfiguredRelationalPersister<C, I> result = polymorphismBuilder.build(dialect, connectionConfiguration, persisterRegistry);
		result = new PersisterListenerWrapper<>(result);
		// We transfer listeners so that all actions are made in the same "event listener context" : all listeners are aggregated in a top level one.
		// Made in particular for already-assigned mark-as-persisted mechanism and relation cascade triggering.
		mainPersister.getPersisterListener().moveTo(result.getPersisterListener());
		
		return result;
	}
}
