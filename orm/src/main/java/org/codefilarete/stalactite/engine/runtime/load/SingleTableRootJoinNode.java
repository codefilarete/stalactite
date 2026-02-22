package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.dsl.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.RootJoinRowConsumer;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * Particular {@link JoinRoot} made to handle single-table polymorphic case : polymorphic entity instantiation is the core focus of it.
 * Identifier is given by the subclass which find its id in the row (see {@link SingleTablePolymorphicJoinRootRowConsumer#findSubInflater(ColumnedRow)}),
 *
 * @author Guillaume Mary
 */
public class SingleTableRootJoinNode<C, I, T extends Table<T>, DTYPE> extends JoinRoot<C, I, T> {
	
	private final Set<? extends ConfiguredRelationalPersister<C, I>> subPersisters;
	private final Set<Column<T, ?>> allColumnsInHierarchy;
	private final Column<T, DTYPE> discriminatorColumn;
	private final SingleTablePolymorphism<C, DTYPE> polymorphismPolicy;
	
	public SingleTableRootJoinNode(EntityJoinTree<C, I> tree,
								   ConfiguredRelationalPersister<C, I> mainPersister,
								   Set<? extends ConfiguredRelationalPersister<C, I>> subPersisters,
								   Column<T, DTYPE> discriminatorColumn,
								   SingleTablePolymorphism<C, DTYPE> polymorphismPolicy) {
		super(tree, new EntityMappingAdapter<>(mainPersister.<T>getMapping()), (T) mainPersister.getMainTable());
		this.subPersisters = subPersisters;
		this.discriminatorColumn = discriminatorColumn;
		this.polymorphismPolicy = polymorphismPolicy;
		this.allColumnsInHierarchy = Collections.cat(Arrays.asList(mainPersister), subPersisters)
				.stream().flatMap(persister -> ((T) persister.getMainTable()).getColumns().stream())
				.collect(Collectors.toCollection(KeepOrderSet::new));
	}
	
	@Override
	public Set<Selectable<?>> getColumnsToSelect() {
		return (Set) this.allColumnsInHierarchy;
	}
	
	@Override
	public RootJoinRowConsumer<C, I> toConsumer(JoinNode<C, T> joinNode) {
		// the decoder can't be a usual variable because it can't be computed now since we lack the EntityTreeInflater context
		// (which is only available when the query is executed, not at build time)
		Supplier<ColumnedRow> decoderProvider = () -> EntityTreeInflater.currentContext().getDecoder(joinNode);
		Set<SubPersisterConsumer<C, I>> subEntityConsumer = subPersisters.stream().map(subPersister -> {
			EntityMapping<C, I, T> mapping = subPersister.getMapping();
			return new SubPersisterConsumer<>(
					row -> mapping.getIdMapping().getIdentifierAssembler().assemble(decoderProvider.get()),
					mapping.getClassToPersist(),
					mapping.getRowTransformer());
		}).collect(Collectors.toSet());
		BiConsumer<C, ColumnedRow> rowConsumptionListener = getConsumptionListener() == null
				? null
				: (rootEntity, row) -> getConsumptionListener().onNodeConsumption(rootEntity, decoderProvider.get());
		return new SingleTablePolymorphicJoinRootRowConsumer<>(joinNode, subEntityConsumer,
				rowConsumptionListener, polymorphismPolicy, row -> decoderProvider.get().get(discriminatorColumn));
	}

	static class SubPersisterConsumer<C, I> {
		private final Function<ColumnedRow, I> identifierAssembler;
		private final Class<C> subEntityType;
		private final RowTransformer<C> subEntityFactory;
		
		private SubPersisterConsumer(Function<ColumnedRow, I> identifierAssembler,
									 Class<C> subEntityType,
									 RowTransformer<C> subEntityFactory) {
			this.identifierAssembler = identifierAssembler;
			this.subEntityType = subEntityType;
			this.subEntityFactory = subEntityFactory;
		}
	}
	
	static class SingleTablePolymorphicJoinRootRowConsumer<C, I, DTYPE> implements RootJoinRowConsumer<C, I> {
		
		private final Set<SubPersisterConsumer<C, I>> subConsumers;
		
		private final JoinNode<C, ?> joinNode;
		/**
		 * Optional listener of ResultSet decoding
		 */
		@Nullable
		private final BiConsumer<C, ColumnedRow> consumptionListener;
		
		private final SingleTablePolymorphism<C, DTYPE> polymorphismPolicy;
		private final Function<ColumnedRow, DTYPE> discriminatorValueReader;
		
		private SingleTablePolymorphicJoinRootRowConsumer(JoinNode<C, ?> node,
														  Set<SubPersisterConsumer<C, I>> subConsumers,
														  @Nullable BiConsumer<C, ColumnedRow> consumptionListener,
														  SingleTablePolymorphism<C, DTYPE> polymorphismPolicy,
														  Function<ColumnedRow, DTYPE> discriminatorValueReader) {
			this.subConsumers = subConsumers;
			this.joinNode = node;
			this.consumptionListener = consumptionListener;
			this.polymorphismPolicy = polymorphismPolicy;
			this.discriminatorValueReader = discriminatorValueReader;
		}

		@Override
		public JoinNode<C, ?> getNode() {
			return joinNode;
		}

		@Override
		public EntityReference<C, I> createRootInstance(ColumnedRow row, TreeInflationContext context) {
			Duo<I, SubPersisterConsumer<C, I>> subInflater = findSubInflater(row);
			EntityReference<C, I> result;
			if (subInflater == null) {
				result = null;
			} else {
				// we don't need a ColumnedRow of sub-entity like in other polymorphic nodes because main persister properties
				// were given to sub-persisters at build time (see SingleTablePolymorphismBuilder#buildSubclassPersister())
				// which is logical since we don't have a join to sub-entity
				C entity = context.giveEntityFromCache(subInflater.getRight().subEntityType, subInflater.getLeft(), () -> subInflater.getRight().subEntityFactory.transform(row));
				result = new EntityReference<>(entity, subInflater.getLeft());
				if (consumptionListener != null) {
					consumptionListener.accept(entity, row);
				}
			}
			return result;
		}
		
		@Nullable
		public Duo<I, SubPersisterConsumer<C, I>> findSubInflater(ColumnedRow row) {
			Class<? extends C> subEntityClass = polymorphismPolicy.getClass(discriminatorValueReader.apply(row));
			Duo<SubPersisterConsumer<C, I>, Class<C>> subClassRowConsumer = Iterables.find(subConsumers, subConsumer -> subConsumer.subEntityType, subEntityClass::equals);
			SubPersisterConsumer<C, I> subIdentifierConsumer = subClassRowConsumer.getLeft();
			return new Duo<>(subIdentifierConsumer.identifierAssembler.apply(row), subIdentifierConsumer);
		}
		
		/**
		 * Implemented for debug. DO NOT RELY ON IT for anything else.
		 */
		@Override
		public String toString() {
			return Reflections.toString(this.getClass());
		}
	}
}
