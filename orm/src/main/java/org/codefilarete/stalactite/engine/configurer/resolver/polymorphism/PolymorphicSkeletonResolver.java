package org.codefilarete.stalactite.engine.configurer.resolver.polymorphism;

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
				// we create a union of the sub-entities tables
				// The union should also contain the join columns for eventual further relation joins. Caution : a first approach was
				// to use the templateReader.getMapping().getSelectableColumns() but it does not contain the join columns which is
				// normal because relations are maintained by ShadowColumnValueProvider, as for many other relations. But, overall,
				// the getMapping().getSelectableColumns() returns the columns of a Union instance created by the
				// TablePerClassPolymorphismReader (to support polymorphism selection) which doesn't contain the relation join columns,
				// because at the time the Reader is created, the relations are not resolved, and, it is very very very hard (understand impossible)
				// to add (or more generally, make visible) the relation join columns to the Union instance later, due to that some objects (PseudoTable)
				// contains some kind of copy of the original columns, and the JoinNode mechanism also maintain some column clones. Globally, trying
				// to fix it is a nightmare, and the only solution is to use the main table columns of the templateReader, which contains all columns (including relation join columns).
				// The identified side effect is if the user gave the Table instance during the DSL usage : this join will contain the eventual superfluous columns
				// that the user maintain himself and are not part of the persistence mapping. This is considered a minor side effect, even more if we consider
				// who uses table-per-class polymorphism ?
				TablePerClassUnion<SUBENTITY, I> tablePerClassUnion = TablePerClassAppender.buildUnion(subReaders.values(), templateReader.getMainTable().getColumns());
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
