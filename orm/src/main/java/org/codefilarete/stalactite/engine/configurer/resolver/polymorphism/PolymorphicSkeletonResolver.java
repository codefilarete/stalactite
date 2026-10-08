package org.codefilarete.stalactite.engine.configurer.resolver.polymorphism;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;

import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.model.EntityPolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.JoinTablePolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.PolymorphicEntity;
import org.codefilarete.stalactite.engine.configurer.model.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.TablePerClassPolymorphism;
import org.codefilarete.stalactite.engine.configurer.resolver.CreatedPersisterCollector;
import org.codefilarete.stalactite.engine.configurer.resolver.DelegatingReadWriteEntityExecutor;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.jointable.JoinTableResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.singletable.SingleTableResolver;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.tableperclass.TablePerClassAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.tableperclass.TablePerClassAppender.TablePerClassUnion;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.tableperclass.TablePerClassResolver;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.PolymorphicWriter;
import org.codefilarete.stalactite.engine.runtime.jointable.JoinTablePolymorphismReader;
import org.codefilarete.stalactite.engine.runtime.singletable.SingleTablePolymorphismReader;
import org.codefilarete.stalactite.engine.runtime.tableperclass.TablePerClassPolymorphismReader;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;

/**
 * Creates a {@link EntityReadWriteExecutor} for a polymorphic entity.
 * Chooses the right resolver according to the polymorphism policy.
 */
public class PolymorphicSkeletonResolver {
	
	private final PersistenceContext persistenceContext;
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	
	public PolymorphicSkeletonResolver(PersistenceContext persistenceContext) {
		this.persistenceContext = persistenceContext;
		this.skeletonAggregateResolver = new SkeletonAggregateResolver(persistenceContext);
	}
	
	public <C, I, T extends Table<T>, SUBENTITY extends C> DelegatingReadWriteEntityExecutor<C, I> resolve(PolymorphicEntity<C, I, T> polymorphicEntity, CreatedPersisterCollector<C, I> persisterCollector) {
		PolymorphismResolver<?> polymorphismResolver = null;
		BiFunction<EntityReader<C, I, T>, Map<Class<SUBENTITY>, EntityReader<SUBENTITY, I, ?>>, ConfiguredEntityReader<C, I, T>> readerBuilder = null;
		EntityPolymorphism<C, I> polymorphism = polymorphicEntity.getPolymorphism();
		if (polymorphism instanceof TablePerClassPolymorphism) {
			polymorphismResolver = new TablePerClassResolver(skeletonAggregateResolver, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
			readerBuilder = (templateReader, subReaders) -> {
				Collection<EntityReader<SUBENTITY, I, ?>> values = subReaders.values();
				TablePerClassUnion<SUBENTITY, I> tablePerClassUnion = TablePerClassAppender.buildUnionForIdentifierRead(values, templateReader.getMapping().getSelectableColumns());
				return new TablePerClassPolymorphismReader<>(templateReader,
						subReaders,
						tablePerClassUnion,
						persistenceContext.getConnectionProvider(),
						persistenceContext.getDialect());
			};
		} else if (polymorphism instanceof JoinTablePolymorphism) {
			polymorphismResolver = new JoinTableResolver(skeletonAggregateResolver, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
			readerBuilder = (templateReader, subReaders) -> new JoinTablePolymorphismReader<>(templateReader,
					subReaders,
					persistenceContext.getConnectionProvider(),
					persistenceContext.getDialect());
		} else if (polymorphism instanceof SingleTablePolymorphism) {
			polymorphismResolver = new SingleTableResolver(skeletonAggregateResolver, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
			readerBuilder = (templateReader, subReaders) -> new SingleTablePolymorphismReader<>(templateReader,
					subReaders,
					(SingleTablePolymorphism<C, I, SUBENTITY, T>) polymorphism,
					persistenceContext.getConnectionProvider(),
					persistenceContext.getDialect());
		}
		PolymorphicWriter<C, I, SUBENTITY> entityWriter = polymorphismResolver.resolve(polymorphicEntity, persisterCollector);
		
		EntityReader<C, I, T> templateReader = new EntityReader<>(entityWriter.<T>getMapping(),
				persistenceContext.getConnectionProvider(),
				persistenceContext.getDialect());
		Map<Class<SUBENTITY>, EntityReader<SUBENTITY, I, ?>> subReaders = buildSubReaders(entityWriter);
		ConfiguredEntityReader<C, I, T> entityReader = readerBuilder.apply(templateReader, subReaders);
		
		DelegatingReadWriteEntityExecutor<C, I> result = new DelegatingReadWriteEntityExecutor<>(entityWriter, entityReader);
		// we overwrite the created persister by the polymorphic one because it was only the template one
		persisterCollector.setPersister(result);
		return result;
	}
	
	/**
	 * Builds one reader per sub-entity type, created from the mapping exposed by the given polymorphism writer.
	 * This factorizes the identical reader-building logic shared by table-per-class, join-table and single-table
	 * polymorphism resolution.
	 *
	 * @param polymorphicWriter the writer holding the abstract entity mapping (the polymorphic root)
	 * @return the sub-entity readers per class
	 */
	private <C, I, SUBENTITY extends C> Map<Class<SUBENTITY>, EntityReader<SUBENTITY, I, ?>> buildSubReaders(PolymorphicWriter<C, I, SUBENTITY> polymorphicWriter) {
		return Iterables.map(polymorphicWriter.getSubEntitiesPersisters().values(),
				EntityWriteExecutor::getClassToPersist,
				subWriter -> {
					EntityMapping<SUBENTITY, I, ?> mapping = subWriter.getMapping();
					return new EntityReader<>(mapping, persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
				});
	}
}
