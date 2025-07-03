package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.ExcludingJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.RootJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode.MergeJoinRowConsumer;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Particular {@link JoinRoot} made to handle join-table polymorphic case : polymorphic entity instantiation is the core focus of it.
 * Here are the hotspots: identifier is given by the subclass which find its id in the row (see {@link JoinTablePolymorphicJoinRootRowConsumer#findSubInflater(ColumnedRow)}),
 * and while doing it, it remembers which consumer made it. Then while instantiating entity we invoke it to get right entity type (parent mapping
 * would only give parent entity, which, out of being a wrong approach, can be an abstract type). Then instance is filled up with parent properties
 * by calling merging method (see line 98).
 * Finally, {@link JoinTablePolymorphicJoinRootRowConsumer} must extend {@link ExcludingJoinRowConsumer} to give next branch to be consumed by
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
	public RootJoinRowConsumer<C> toConsumer(JoinNode<T> joinNode) {
		RowTransformer<C> rootRowTransformer = getEntityInflater().getRowTransformer();
		Set<SubPersisterConsumer<C, I>> subEntityConsumer = subPersisters.stream().map(subPersister -> {
			EntityMapping<C, I, T> mapping = subPersister.getMapping();
			return new SubPersisterConsumer<>(
					row -> mapping.getIdMapping().getIdentifierAssembler().assemble(row),
					mapping.getClassToPersist(),
					mapping.getRowTransformer());
		}).collect(Collectors.toSet());
		rootConsumer = new JoinTablePolymorphicJoinRootRowConsumer<>(joinNode, rootRowTransformer, subEntityConsumer,
				getConsumptionListener() == null ? null : (rootEntity, row) -> getConsumptionListener().onNodeConsumption(rootEntity, EntityTreeInflater.currentContext().getDecoder(joinNode)::get));
		return rootConsumer;
	}
	
	public void addSubPersister(ConfiguredRelationalPersister<C, I> persister, MergeJoinRowConsumer<C> subConsumer) {
		rootConsumer.subConsumers.forEach(pawnConsumer -> {
			if (pawnConsumer.subEntityType == persister.getClassToPersist()) {
				pawnConsumer.subPropertiesApplier = subConsumer;
				pawnConsumer.identifierAssembler = row -> persister.getMapping().getIdMapping().getIdentifierAssembler().assemble(row);
			}
		});
	}
	
	static class SubPersisterConsumer<C, I> {
		private Function<ColumnedRow, I> identifierAssembler;
		private final Class<C> subEntityType;
		private final RowTransformer<C> subEntityFactory;
		private MergeJoinRowConsumer<C> subPropertiesApplier;
		
		private SubPersisterConsumer(Function<ColumnedRow, I> identifierAssembler,
									 Class<C> subEntityType,
									 RowTransformer<C> subEntityFactory) {
			this.identifierAssembler = identifierAssembler;
			this.subEntityType = subEntityType;
			this.subEntityFactory = subEntityFactory;
		}
	}
	
	static class JoinTablePolymorphicJoinRootRowConsumer<C, I> implements ExcludingJoinRowConsumer<C> {
		
		private static final ThreadLocal<MergeJoinRowConsumer<?>> CURRENTLY_FOUND_CONSUMER = new ThreadLocal<>();
		
		protected final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
		
		private final JoinNode<?> node;
		private final RowTransformer<C> rootRowTransformer;
		private final Set<SubPersisterConsumer<C, I>> subConsumers;
		
		/** Optional listener of ResultSet decoding */
		@Nullable
		private final BiConsumer<C, ColumnedRow> consumptionListener;
		
		private JoinTablePolymorphicJoinRootRowConsumer(JoinNode<?> node,
														RowTransformer<C> rootRowTransformer,
														Set<SubPersisterConsumer<C, I>> subConsumers,
														@Nullable BiConsumer<C, ColumnedRow> consumptionListener) {
			this.node = node;
			this.rootRowTransformer = rootRowTransformer;
			this.subConsumers = subConsumers;
			this.consumptionListener = consumptionListener;
		}
		
		@Override
		public JoinNode<?> getNode() {
			return node;
		}

		@Override
		public C createRootInstance(ColumnedRow row, TreeInflationContext context) {
			Duo<I, SubPersisterConsumer<C, I>> subInflater = findSubInflater(row);
			C result;
			if (subInflater == null) {
				CURRENTLY_FOUND_CONSUMER.remove();
				result = null;
			} else {
				CURRENTLY_FOUND_CONSUMER.set(subInflater.getRight().subPropertiesApplier);
				result = (C) context.giveEntityFromCache(subInflater.getRight().subEntityType, subInflater.getLeft(), () -> {
					LOGGER.debug("Instantiating entity of type {}", subInflater.getRight().subEntityType);
					ColumnedRow subInflaterRow = EntityTreeInflater.currentContext().getDecoder(subInflater.getRight().subPropertiesApplier.getNode());
					// by using transform(..) we instantiate the right type (the sub-entity one) and fill it with sub-entity properties
					return subInflater.getRight().subEntityFactory.transform(subInflaterRow);
				});
				// applying parent properties to the entity
				rootRowTransformer.applyRowToBean(row, result);
			}
			if (consumptionListener != null) {
				consumptionListener.accept(result, row);
			}
			return result;
		}
		
		@Nullable
		/* Optimized, from 530 000 nanos to 65 000 nanos at 1st exec, from 40 000 nanos to 12 000 nanos on usual run */
		public Duo<I, SubPersisterConsumer<C, I>> findSubInflater(ColumnedRow row) {
			// @Optimized : use for & return instead of stream().map().filter(notNull).findFirst()
			for (SubPersisterConsumer<C, I> pawn : subConsumers) {
				ColumnedRow subInflaterRow = EntityTreeInflater.currentContext().getDecoder(pawn.subPropertiesApplier.getNode());
				I assemble = pawn.identifierAssembler.apply(subInflaterRow);
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
		
		/**
		 * Implemented for debug. DO NOT RELY ON IT for anything else.
		 */
		@Override
		public String toString() {
			return Reflections.toString(this.getClass());
		}
	}
}