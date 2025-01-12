package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.ForkJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.RootJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode.MergeJoinRowConsumer;
import org.codefilarete.stalactite.mapping.AbstractTransformer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Duo;

/**
 * Particular {@link JoinRoot} made to handle join-table polymorphic case : polymorphic entity instantiation is the core focus of it.
 * Here are the hotspots: identifier is given by the subclass which find its id in the row (see {@link JoinTablePolymorphicJoinRootRowConsumer#findSubInflater(Row)}),
 * and while doing it, it remembers which consumer made it. Then while instantiating entity we invoke it to get right entity type (parent mapping
 * would only give parent entity, which, out of being a wrong approach, can be an abstract type). Then instance is filled up with parent properties
 * by calling merging method (see line 98).
 * Finally, {@link JoinTablePolymorphicJoinRootRowConsumer} must extend {@link ForkJoinRowConsumer} to give next branch to be consumed by
 * {@link EntityTreeInflater} to avoid "dead" branch to be read : we give it according to the consumer which found the identifier. 
 * 
 * @author Guillaume Mary
 */
public class JoinTableRootJoinNode<C, I, T extends Table<T>> extends JoinRoot<C, I, T> {
	
	private final Set<? extends ConfiguredRelationalPersister<C, I>> subPersisters;
	private final Set<Column<T, ?>> selectableColumns;
	private JoinTablePolymorphicJoinRootRowConsumer<C, I> rootConsumer;
	
	public JoinTableRootJoinNode(EntityJoinTree<C, I> tree,
								 ConfiguredRelationalPersister<C, I> mainPersister,
								 Set<? extends ConfiguredRelationalPersister<C, I>> subPersisters,
								 Set<? extends Column<T, ?>> selectableColumns,
								 T mainTable) {
		super(tree, new EntityMappingAdapter<>(mainPersister.<T>getMapping()), mainTable);
		this.subPersisters = subPersisters;
		this.selectableColumns = (Set<Column<T, ?>>) selectableColumns;
	}
	
	@Override
	public Set<Selectable<?>> getColumnsToSelect() {
		return (Set) this.selectableColumns;
	}
	
	@Override
	public RootJoinRowConsumer<C> toConsumer(ColumnedRow columnedRow) {
		RowTransformer<C> rootRowTransformer = getEntityInflater().copyTransformerWithAliases(columnedRow);
		Set<SubPersisterConsumer<C, I>> subEntityConsumer = subPersisters.stream().map(subPersister -> {
			EntityMapping<C, I, T> mapping = subPersister.getMapping();
			return new SubPersisterConsumer<>(
					row -> mapping.getIdMapping().getIdentifierAssembler().assemble(row, columnedRow),
					mapping.getClassToPersist(),
					(AbstractTransformer<C>) mapping.copyTransformerWithAliases(columnedRow));
		}).collect(Collectors.toSet());
		rootConsumer = new JoinTablePolymorphicJoinRootRowConsumer<>(rootRowTransformer, subEntityConsumer,
				getConsumptionListener() == null ? null : (rootEntity, row) -> getConsumptionListener().onNodeConsumption(rootEntity, column -> columnedRow.getValue(column, row)));
		return rootConsumer;
	}
	
	public void addSubPersister(ConfiguredRelationalPersister<C, I> persister, MergeJoinRowConsumer<C> subConsumer, ColumnedRow columnedRow) {
		rootConsumer.subConsumers.forEach(pawnConsumer -> {
			if (pawnConsumer.subEntityType == persister.getClassToPersist()) {
				pawnConsumer.subPropertiesApplier = subConsumer;
				pawnConsumer.identifierAssembler = row -> persister.getMapping().getIdMapping().getIdentifierAssembler().assemble(row, columnedRow);
			}
		});
	}
	
	static class SubPersisterConsumer<C, I> {
		private Function<Row, I> identifierAssembler;
		private final Class<C> subEntityType;
		private final AbstractTransformer<C> subEntityFactory;
		private MergeJoinRowConsumer<C> subPropertiesApplier;
		
		private SubPersisterConsumer(Function<Row, I> identifierAssembler,
									 Class<C> subEntityType,
									 AbstractTransformer<C> subEntityFactory) {
			this.identifierAssembler = identifierAssembler;
			this.subEntityType = subEntityType;
			this.subEntityFactory = subEntityFactory;
		}
	}
	
	static class JoinTablePolymorphicJoinRootRowConsumer<C, I> implements RootJoinRowConsumer<C> {
		
		private static final ThreadLocal<MergeJoinRowConsumer<?>> CURRENTLY_FOUND_CONSUMER = new ThreadLocal<>();
		
		private final RowTransformer<C> rootRowTransformer;
		private final Set<SubPersisterConsumer<C, I>> subConsumers;
		
		/** Optional listener of ResultSet decoding */
		@Nullable
		private final BiConsumer<C, Row> consumptionListener;
		
		private JoinTablePolymorphicJoinRootRowConsumer(RowTransformer<C> rootRowTransformer,
														Set<SubPersisterConsumer<C, I>> subConsumers,
														@Nullable BiConsumer<C, Row> consumptionListener) {
			this.rootRowTransformer = rootRowTransformer;
			this.subConsumers = subConsumers;
			this.consumptionListener = consumptionListener;
		}
		
		@Override
		public C createRootInstance(Row row, TreeInflationContext context) {
			Duo<I, SubPersisterConsumer<C, I>> subInflater = findSubInflater(row);
			C result;
			if (subInflater == null) {
				CURRENTLY_FOUND_CONSUMER.remove();
				result = null;
			} else {
				CURRENTLY_FOUND_CONSUMER.set(subInflater.getRight().subPropertiesApplier);
				result = (C) context.giveEntityFromCache(subInflater.getRight().subEntityType, subInflater.getLeft(), () -> subInflater.getRight().subEntityFactory.newBeanInstance(row));
				rootRowTransformer.applyRowToBean(row, result);
			}
			if (consumptionListener != null) {
				consumptionListener.accept(result, row);
			}
			return result;
		}
		
		@Nullable
		/* Optimized, from 530 000 nanos to 65 000 nanos at 1st exec, from 40 000 nanos to 12 000 nanos on usual run */
		public Duo<I, SubPersisterConsumer<C, I>> findSubInflater(Row row) {
			// @Optimized : use for & return instead of stream().map().filter(notNull).findFirst()
			for (SubPersisterConsumer<C, I> pawn : subConsumers) {
				I assemble = pawn.identifierAssembler.apply(row);
				if (assemble != null) {
					return new Duo<>(assemble, pawn);
				}
			}
			return null;
		}
		
		/**
		 * Gives the row consumers that instantiates sub-entities but weren't detected in very previous identifier detection.
		 * This allows to keep effective sub-entity merger and relation consumers of parent entity while iterating of joins during tree inflation.
		 * 
		 * @return the consumers that shouldn't be taken into account in next tree iteration
		 */
		public Set<JoinRowConsumer> giveExcludedConsumers() {
			return subConsumers.stream()
					.map(subConsumer -> subConsumer.subPropertiesApplier)
					.filter(consumerPawn -> CURRENTLY_FOUND_CONSUMER.get() != consumerPawn)
					.collect(Collectors.toSet());
		}
	}
}