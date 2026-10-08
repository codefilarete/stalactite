package org.codefilarete.stalactite.engine.runtime.load;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.codefilarete.stalactite.engine.EntityReadExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.ExcludingJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.RootJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode.MergeJoinRowConsumer;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.api.QueryStatement.PseudoTable;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.api.Selectable.SimpleSelectable;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Particular {@link JoinRoot} made to handle table-per-class polymorphic case: polymorphic entity instantiation is the core focus of it.
 * DISCLAIMER: keep in mind that caller built the query with a Union of sub-tables with the only common properties of the hierarchy, and joins it with
 * sub-tables to be able to build a whole instance.
 * 
 * Here are the hotspots: the appropriate sub-class to instantiate is provided through the discriminator took in the union (see
 * {@link TablePerClassPolymorphicJoinRootRowConsumer#findSubInflater(ColumnedRow)}), then the identifier is given by the subclass matching the discriminator
 * in the row, and while doing it, it remembers which consumer made it. Then while instantiating the entity, we invoke it to get the right entity type
 * (parent mapping would only give parent entity, which, out of being a wrong approach, can be an abstract type). Then instance is filled up by the
 * appropriate join consumer selected thanks to {@link ExcludingJoinRowConsumer#giveExcludedConsumers()} invoked by
 * {@link EntityTreeInflater#getRootEntityCreationResult(ColumnedRow, TreeInflationContext)}
 * @author Guillaume Mary
 */
public class TablePerClassRootJoinNode<C, I> extends JoinRoot<C, I, PseudoTable> {

	private final Map<Integer, EntityReadExecutor<? extends C, I>> subPersisters;
	private final SimpleSelectable<Integer> discriminatorColumn;
	private TablePerClassPolymorphicJoinRootRowConsumer<C, I> rootConsumer;
	
	public <SUBENTITY extends C> TablePerClassRootJoinNode(EntityJoinTree<C, I> tree,
														   EntityMapping<C, I, ?> mainMapping,
														   Map<Integer, ? extends EntityReadExecutor<SUBENTITY, I>> subPersisters,
														   PseudoTable union,
														   SimpleSelectable<Integer> discriminatorColumn) {
		super(tree, new EntityMappingAdapter<>(mainMapping), union);
		this.subPersisters = new HashMap<>(subPersisters);
		this.discriminatorColumn = discriminatorColumn;
	}
	
	@Override
	public Set<Selectable<?>> getColumnsToSelect() {
		return (Set) getTable().getColumns();
	}
	
	@Override
	public RootJoinRowConsumer<C, I> toConsumer(JoinNode<C, PseudoTable> joinNode) {
		Set<SubPersisterConsumer<C, I>> subEntityConsumers = buildSubEntityConsumers();
		rootConsumer = new TablePerClassPolymorphicJoinRootRowConsumer<>(
				joinNode,
				subEntityConsumers,
				getConsumptionListener() == null
						? null
						: (rootEntity, row) -> getConsumptionListener().onNodeConsumption(rootEntity, EntityTreeInflater.currentContext().getDecoder(joinNode)::get),
				row -> row.get(joinNode.getTable().findColumn(discriminatorColumn.getExpression())));
		return rootConsumer;
	}
	
	private <SUBENTITY extends C> Set<SubPersisterConsumer<SUBENTITY, I>> buildSubEntityConsumers() {
		return subPersisters.values().stream().map(subPersister -> {
			EntityMapping<SUBENTITY, I, ?> mapping = (EntityMapping<SUBENTITY, I, ?>) subPersister.getMapping();
			IdentifierAssembler<I, ?> identifierAssembler = mapping.getIdMapping().getIdentifierAssembler();
			return new SubPersisterConsumer<>(
					identifierAssembler::assemble,
					mapping.getClassToPersist(),
					mapping.getRowTransformer());
		}).collect(Collectors.toSet());
	}
	
	public void addSubPersister(EntityReadExecutor<? extends C, I> persister, MergeJoinRowConsumer<? extends C> subConsumer, Integer discriminatorValue) {
		rootConsumer.subConsumers.forEach(pawnConsumer -> {
			if (pawnConsumer.subEntityType == persister.getClassToPersist()) {
				pawnConsumer.subPropertiesApplier = subConsumer;
				pawnConsumer.discriminatorValue = discriminatorValue;
			}
		});
	}
	
	static class SubPersisterConsumer<C, I> {
		private final Function<ColumnedRow, I> identifierAssembler;
		private final Class<C> subEntityType;
		private final RowTransformer<C> subEntityFactory;
		private MergeJoinRowConsumer<? extends C> subPropertiesApplier;
		private Integer discriminatorValue;
		
		private SubPersisterConsumer(Function<ColumnedRow, I> identifierAssembler,
									 Class<C> subEntityType,
									 RowTransformer<C> subEntityFactory) {
			this.identifierAssembler = identifierAssembler;
			this.subEntityType = subEntityType;
			this.subEntityFactory = subEntityFactory;
		}
	}
	
	static class TablePerClassPolymorphicJoinRootRowConsumer<C, I> implements ExcludingJoinRowConsumer<C, I> {
		
		private static final ThreadLocal<MergeJoinRowConsumer<?>> CURRENTLY_FOUND_CONSUMER = new ThreadLocal<>();
		
		private final JoinNode<C, ?> node;
		private final Set<SubPersisterConsumer<C, I>> subConsumers;
		
		/** Optional listener of ResultSet decoding */
		@Nullable
		private final BiConsumer<C, ColumnedRow> consumptionListener;
		private final Function<ColumnedRow, Integer> discriminatorValueReader;
		
		private TablePerClassPolymorphicJoinRootRowConsumer(
				JoinNode<C, ?> node,
				Set<SubPersisterConsumer<C, I>> subConsumers,
				@Nullable BiConsumer<C, ColumnedRow> consumptionListener,
				Function<ColumnedRow, Integer> discriminatorValueReader) {
			this.node = node;
			this.subConsumers = subConsumers;
			this.consumptionListener = consumptionListener;
			this.discriminatorValueReader = discriminatorValueReader;
		}

		@Override
		public JoinNode<C, ?> getNode() {
			return node;
		}
		
		@Override
		public EntityReference<C, I> createRootInstance(ColumnedRow row, TreeInflationContext context) {
			SubPersisterConsumer<C, I> subInflater = findSubInflater(row);
			EntityReference<C, I> result;
			if (subInflater == null) {
				CURRENTLY_FOUND_CONSUMER.remove();
				result = null;
			} else {
				CURRENTLY_FOUND_CONSUMER.set(subInflater.subPropertiesApplier);
				ColumnedRow subInflaterRow = EntityTreeInflater.currentContext().getDecoder(subInflater.subPropertiesApplier.getNode());
				I identifier = subInflater.identifierAssembler.apply(subInflaterRow);
				C entity = context.giveEntityFromCache(subInflater.subEntityType, identifier, () -> subInflater.subEntityFactory.transform(subInflaterRow));
				result = new EntityReference<>(entity, identifier);
				if (consumptionListener != null) {
					consumptionListener.accept(entity, row);
				}
			}
			return result;
		}
		
		@Nullable
		public SubPersisterConsumer<C, I> findSubInflater(ColumnedRow row) {
			// NB: we trim because some database (as HSQLDB) adds some padding in order that all values get same length
			Integer discriminatorValue = nullable(discriminatorValueReader.apply(row)).get();
			return Iterables.find(subConsumers,
					subConsumer -> subConsumer.discriminatorValue.equals(discriminatorValue));
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
