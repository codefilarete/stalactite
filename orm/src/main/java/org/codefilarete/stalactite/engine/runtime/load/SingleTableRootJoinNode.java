package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.RootJoinRowConsumer;
import org.codefilarete.stalactite.mapping.AbstractTransformer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * Particular {@link JoinRoot} made to handle single-table polymorphic case : polymorphic entity instantiation is the core focus of it.
 * Identifier is given by the subclass which find its id in the row (see {@link SingleTablePolymorphicJoinRootRowConsumer#findSubInflater(Row)}),
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
	public RootJoinRowConsumer<C> toConsumer(ColumnedRow columnedRow) {
		Set<SubPersisterConsumer<C, I>> subEntityConsumer = subPersisters.stream().map(subPersister -> {
			EntityMapping<C, I, T> mapping = subPersister.getMapping();
			return new SubPersisterConsumer<>(
					row -> mapping.getIdMapping().getIdentifierAssembler().assemble(row, columnedRow),
					mapping.getClassToPersist(),
					(AbstractTransformer<C>) mapping.copyTransformerWithAliases(columnedRow));
		}).collect(Collectors.toSet());
		BiConsumer<C, Row> rowConsumptionListener = getConsumptionListener() == null
				? null
				: (rootEntity, row) -> getConsumptionListener().onNodeConsumption(rootEntity, column -> columnedRow.getValue(column, row));
		return new SingleTablePolymorphicJoinRootRowConsumer<>(subEntityConsumer,
				rowConsumptionListener, polymorphismPolicy, row -> columnedRow.getValue(discriminatorColumn, row));
	}

	static class SubPersisterConsumer<C, I> {
		private final Function<Row, I> identifierAssembler;
		private final Class<C> subEntityType;
		private final AbstractTransformer<C> subEntityFactory;
		
		private SubPersisterConsumer(Function<Row, I> identifierAssembler,
									 Class<C> subEntityType,
									 AbstractTransformer<C> subEntityFactory) {
			this.identifierAssembler = identifierAssembler;
			this.subEntityType = subEntityType;
			this.subEntityFactory = subEntityFactory;
		}
	}
	
	static class SingleTablePolymorphicJoinRootRowConsumer<C, I, DTYPE> implements RootJoinRowConsumer<C> {
		
		private final Set<SubPersisterConsumer<C, I>> subConsumers;
		
		/**
		 * Optional listener of ResultSet decoding
		 */
		@Nullable
		private final BiConsumer<C, Row> consumptionListener;
		
		private final SingleTablePolymorphism<C, DTYPE> polymorphismPolicy;
		private final Function<Row, DTYPE> discriminatorValueReader;
		
		private SingleTablePolymorphicJoinRootRowConsumer(Set<SubPersisterConsumer<C, I>> subConsumers,
														  @Nullable BiConsumer<C, Row> consumptionListener,
														  SingleTablePolymorphism<C, DTYPE> polymorphismPolicy,
														  Function<Row, DTYPE> discriminatorValueReader) {
			this.subConsumers = subConsumers;
			this.consumptionListener = consumptionListener;
			this.polymorphismPolicy = polymorphismPolicy;
			this.discriminatorValueReader = discriminatorValueReader;
		}
		
		@Override
		public C createRootInstance(Row row, TreeInflationContext context) {
			Duo<I, SubPersisterConsumer<C, I>> subInflater = findSubInflater(row);
			C result;
			if (subInflater == null) {
				result = null;
			} else {
				result = (C) context.giveEntityFromCache(subInflater.getRight().subEntityType, subInflater.getLeft(), () -> subInflater.getRight().subEntityFactory.transform(row));
			}
			if (consumptionListener != null) {
				consumptionListener.accept(result, row);
			}
			return result;
		}
		
		@Nullable
		public Duo<I, SubPersisterConsumer<C, I>> findSubInflater(Row row) {
			Class<? extends C> subEntityClass = polymorphismPolicy.getClass(discriminatorValueReader.apply(row));
			Duo<SubPersisterConsumer<C, I>, Class<C>> subClassRowConsumer = Iterables.find(subConsumers, subConsumer -> subConsumer.subEntityType, subEntityClass::equals);
			SubPersisterConsumer<C, I> subIdentifierConsumer = subClassRowConsumer.getLeft();
			return new Duo<>(subIdentifierConsumer.identifierAssembler.apply(row), subIdentifierConsumer);
		}
	}
}