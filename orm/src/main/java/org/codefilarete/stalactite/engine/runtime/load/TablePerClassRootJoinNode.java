package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.Map;
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
import org.codefilarete.stalactite.mapping.AbstractTransformer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.query.model.QueryStatement.PseudoTable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Selectable.SelectableString;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Particular {@link JoinRoot} made to handle table-per-class polymorphic case: polymorphic entity instantiation is the core focus of it.
 * DISCLAIMER: keep in mind that caller built the query with a Union of sub-tables with the only common properties of the hierarchy, and joins it with
 * sub-tables to be able to build a whole instance.
 * 
 * Here are the hotspots: the appropriate sub-class to instantiate is provided through the discriminator took in the union (see
 * {@link TablePerClassPolymorphicJoinRootRowConsumer#findSubInflater(Row)}), then the identifier is given by the subclass matching the discriminator
 * in the row, and while doing it, it remembers which consumer made it. Then while instantiating the entity, we invoke it to get the right entity type
 * (parent mapping would only give parent entity, which, out of being a wrong approach, can be an abstract type). Then instance is filled up by the
 * appropriate join consumer selected thanks to {@link ExcludingJoinRowConsumer#giveExcludedConsumers()} invoked by
 * {@link EntityTreeInflater#getRootEntityCreationResult(Row, TreeInflationContext)}
 * @author Guillaume Mary
 */
public class TablePerClassRootJoinNode<C, I> extends JoinRoot<C, I, PseudoTable> {
	
	private final Map<String, ConfiguredRelationalPersister<C, I>> subPersisters;
	private final SelectableString<String> discriminatorColumn;
	private TablePerClassPolymorphicJoinRootRowConsumer<C, I> rootConsumer;
	
	public TablePerClassRootJoinNode(EntityJoinTree<C, I> tree,
									 ConfiguredRelationalPersister<C, I> mainPersister,
									 Map<String, ConfiguredRelationalPersister<C, I>> subPersisters,
									 PseudoTable union,
									 SelectableString<String> discriminatorColumn) {
		super(tree, new EntityMappingAdapter<>(mainPersister.<Table>getMapping()), union);
		this.subPersisters = subPersisters;
		this.discriminatorColumn = discriminatorColumn;
	}
	
	@Override
	public Set<Selectable<?>> getColumnsToSelect() {
		return (Set) getTable().getColumns();
	}
	
	@Override
	public RootJoinRowConsumer<C> toConsumer(ColumnedRow columnedRow) {
		Set<SubPersisterConsumer<C, I>> subEntityConsumers = subPersisters.values().stream().map(subPersister -> {
			EntityMapping<C, I, ?> mapping = subPersister.getMapping();
			return new SubPersisterConsumer<>(
					row -> mapping.getIdMapping().getIdentifierAssembler().assemble(row, columnedRow),
					mapping.getClassToPersist(),
					(AbstractTransformer<C>) mapping.copyTransformerWithAliases(columnedRow));
		}).collect(Collectors.toSet());
		rootConsumer = new TablePerClassPolymorphicJoinRootRowConsumer<>(
				subEntityConsumers,
				getConsumptionListener() == null
						? null
						: (rootEntity, row) -> getConsumptionListener().onNodeConsumption(rootEntity, column -> columnedRow.getValue(column, row)),
				row -> columnedRow.getValue(discriminatorColumn, row));
		return rootConsumer;
	}
	
	public void addSubPersister(ConfiguredRelationalPersister<C, I> persister, MergeJoinRowConsumer<? extends C> subConsumer, String discriminatorValue, ColumnedRow columnedRow) {
		rootConsumer.subConsumers.forEach(pawnConsumer -> {
			if (pawnConsumer.subEntityType == persister.getClassToPersist()) {
				pawnConsumer.subPropertiesApplier = subConsumer;
				pawnConsumer.discriminatorValue = discriminatorValue;
			}
		});
	}
	
	static class SubPersisterConsumer<C, I> {
		private final Function<Row, I> identifierAssembler;
		private final Class<C> subEntityType;
		private final AbstractTransformer<C> subEntityFactory;
		private MergeJoinRowConsumer<? extends C> subPropertiesApplier;
		private String discriminatorValue;
		
		private SubPersisterConsumer(Function<Row, I> identifierAssembler,
									 Class<C> subEntityType,
									 AbstractTransformer<C> subEntityFactory) {
			this.identifierAssembler = identifierAssembler;
			this.subEntityType = subEntityType;
			this.subEntityFactory = subEntityFactory;
		}
	}
	
	static class TablePerClassPolymorphicJoinRootRowConsumer<C, I> implements ExcludingJoinRowConsumer<C> {
		
		private static final ThreadLocal<MergeJoinRowConsumer<?>> CURRENTLY_FOUND_CONSUMER = new ThreadLocal<>();
		
		private final Set<SubPersisterConsumer<C, I>> subConsumers;
		
		/** Optional listener of ResultSet decoding */
		@Nullable
		private final BiConsumer<C, Row> consumptionListener;
		private final Function<Row, String> discriminatorValueReader;
		
		private TablePerClassPolymorphicJoinRootRowConsumer(
				Set<SubPersisterConsumer<C, I>> subConsumers,
				@Nullable BiConsumer<C, Row> consumptionListener,
				Function<Row, String> discriminatorValueReader) {
			this.subConsumers = subConsumers;
			this.consumptionListener = consumptionListener;
			this.discriminatorValueReader = discriminatorValueReader;
		}
		
		@Override
		public C createRootInstance(Row row, TreeInflationContext context) {
			SubPersisterConsumer<C, I> subInflater = findSubInflater(row);
			C result;
			if (subInflater == null) {
				CURRENTLY_FOUND_CONSUMER.remove();
				result = null;
			} else {
				CURRENTLY_FOUND_CONSUMER.set(subInflater.subPropertiesApplier);
				I identifier = subInflater.identifierAssembler.apply(row);
				result = (C) context.giveEntityFromCache(subInflater.subEntityType, identifier, () -> subInflater.subEntityFactory.newBeanInstance(row));
			}
			if (consumptionListener != null) {
				consumptionListener.accept(result, row);
			}
			return result;
		}
		
		@Nullable
		public SubPersisterConsumer<C, I> findSubInflater(Row row) {
			// NB: we trim because some database (as HSQLDB) adds some padding in order that all values get same length
			String discriminatorValue = nullable(discriminatorValueReader.apply(row)).map(String::trim).get();
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