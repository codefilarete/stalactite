package org.codefilarete.stalactite.engine.runtime.onetomany;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * Container to store information of an indexed *-to-many indexed mapped relation (by a column on the reverse side)
 * 
 * @author Guillaume Mary
 */
public class IndexedMappedManyRelationDescriptor<SRC, TRGT, C extends Collection<TRGT>, SRCID, TRGTID> extends MappedManyRelationDescriptor<SRC, TRGT, C, SRCID> {
	
	/** Column that stores index value, owned by reverse side table (table of targetPersister) */
	private final Column<Table, Integer> indexingColumn;
	
	public IndexedMappedManyRelationDescriptor(Accessor<SRC, C> collectionGetter,
											   BiConsumer<SRC, C> collectionSetter,
											   Supplier<C> collectionFactory,
											   @Nullable BiConsumer<TRGT, SRC> reverseSetter,
											   Key<?, SRCID> reverseColumn,
											   Column<? extends Table, Integer> indexingColumn,
											   Function<SRC, SRCID> idProvider,
											   Function<TRGT, TRGTID> targetIdProvider) {
		super(collectionGetter, collectionSetter, collectionFactory, reverseSetter, reverseColumn);
		this.indexingColumn = (Column<Table, Integer>) indexingColumn;
		super.relationFixer = new InMemoryRelationHolder(idProvider, targetIdProvider);
	}
	
	public Column<Table, Integer> getIndexingColumn() {
		return indexingColumn;
	}
	
	/**
	 * A relation fixer that doesn't set the Collection onto the source bean but keeps it in memory.
	 * Made for collection with persisted order: it is sorted later (by some other code) by getting the collection
	 * with {@link #get(Object)}, and then set it onto source bean (by collection setter).
	 * Collection is kept in memory thanks to a ThreadLocal because its lifetime is expected to be a select execution,
	 * hence caller is expected to initialize and clean this instance by calling {@link #init()} and {@link #clear()}
	 * before and after selection.
	 * 
	 * @see #applySort(Set)
	 * @author Guillaume Mary
	 */
	public class InMemoryRelationHolder implements BeanRelationFixer<SRC, TRGT> {
		
		// Note that we prefer to store Map<SRCID> instead of IdentityMap for now because it's simpler but requires identifier providers.
		// Could be replaced by IdentityMap<SRC>
		private final ThreadLocal<Map<SRCID, List<TRGT>>> relationCollectionPerEntity = new ThreadLocal<>();
		
		/**
		 * Context for indexed mapped List. Will keep bean index during select between "unrelated" methods/phases :
		 * index column must be added to SQL select, read from ResultSet and order applied to sort final List, but this particular feature crosses over
		 * layers (entities and SQL) which is not implemented. In such circumstances, ThreadLocal comes to the rescue.
		 * Could be static, but would lack the TRGTID typing, which leads to some generics errors, so left non-static (acceptable small overhead)
		 */
		private final ThreadLocal<Map<TRGTID, Integer>> currentSelectedIndexes = new ThreadLocal<>();
		
		private final Function<SRC, SRCID> idProvider;
		private final Function<TRGT, TRGTID> targetIdProvider;
		
		public InMemoryRelationHolder(Function<SRC, SRCID> idProvider,
									  Function<TRGT, TRGTID> targetIdProvider) {
			this.idProvider = idProvider;
			this.targetIdProvider = targetIdProvider;
		}
		
		public Map<TRGTID, Integer> getCurrentSelectedIndexes() {
			return currentSelectedIndexes.get();
		}
		
		@Override
		public void apply(SRC source, TRGT input) {
			Map<SRCID, List<TRGT>> srcidcMap = relationCollectionPerEntity.get();
			List<TRGT> collection = srcidcMap.computeIfAbsent(idProvider.apply(source), id -> new LinkedList<>());
			collection.add(input);
			// bidirectional assignment
			preventNull(getReverseSetter(), NOOP_REVERSE_SETTER).accept(input, source);
		}
		
		private List<TRGT> get(SRC src) {
			Map<SRCID, List<TRGT>> currentMap = relationCollectionPerEntity.get();
			return currentMap == null ? null : currentMap.get(idProvider.apply(src));
		}
		
		public void init() {
			this.relationCollectionPerEntity.set(new HashMap<>());
			this.currentSelectedIndexes.set(new HashMap<>());
		}
		
		public void clear() {
			this.relationCollectionPerEntity.remove();
			this.currentSelectedIndexes.remove();
		}
		
		/**
		 * Reconciliate {@link Collection} order.
		 * To be called after selecting entities from the database.
		 * 
		 * @param result the entities to be sorted
		 */
		public void applySort(Set<? extends SRC> result) {
			result.forEach(src -> {
				List<TRGT> inMemoryCollection = get(src);
				if (inMemoryCollection != null) {	// inMemoryCollection can be null if there's no associated entity in the database
					Map<TRGTID, Integer> indexPerTargetId = currentSelectedIndexes.get();
					inMemoryCollection.sort(Comparator.comparingInt(target -> indexPerTargetId.get(targetIdProvider.apply(target))));
					C relationCollection = getCollectionGetter().apply(src);
					if (relationCollection == null) {
						relationCollection = getCollectionFactory().get();
						getCollectionSetter().accept(src, relationCollection);
					}
					relationCollection.addAll(inMemoryCollection);
				}
			});
		}
	}
}
