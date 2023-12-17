package org.codefilarete.stalactite.engine.runtime.onetomany;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * Container to store information of a one-to-many indexed mapped relation (by a column on the reverse side)
 * 
 * @author Guillaume Mary
 */
public class IndexedMappedManyRelationDescriptor<SRC, TRGT, C extends Collection<TRGT>, SRCID> extends MappedManyRelationDescriptor<SRC, TRGT, C, SRCID> {
	
	/** Column that stores index value, owned by reverse side table (table of targetPersister) */
	private final Column<Table, Integer> indexingColumn;
	
	public IndexedMappedManyRelationDescriptor(Function<SRC, C> collectionGetter,
											   BiConsumer<SRC, C> collectionSetter,
											   Supplier<C> collectionFactory,
											   BiConsumer<TRGT, SRC> reverseSetter,
											   Key<?, SRCID> reverseColumn,
											   Column<? extends Table, Integer> indexingColumn,
											   Function<SRC, SRCID> idProvider) {
		super(collectionGetter, collectionSetter, collectionFactory, reverseSetter, reverseColumn);
		this.indexingColumn = (Column<Table, Integer>) indexingColumn;
		super.relationFixer = new InMemoryRelationHolder(idProvider);
	}
	
	public Column<Table, Integer> getIndexingColumn() {
		return indexingColumn;
	}
	
	/**
	 * A relation fixer that doesn't set collection onto source bean but keeps it in memory.
	 * Made for collection with persisted order : it is sorted later (by some other code) by getting the collection
	 * with {@link #get(Object)}, and then set it onto source bean (by collection setter).
	 * Collection is kept in memory thanks to a ThreadLocal because its lifetime is expected to be a select execution,
	 * hence caller is expected to initialise and clean this instance by calling {@link #init()} and {@link #clear()}
	 * before and after selection.
	 * 
	 * @author Guillaume Mary
	 */
	public class InMemoryRelationHolder implements BeanRelationFixer<SRC, TRGT> {
		
		private final ThreadLocal<Map<SRCID, C>> relationCollectionPerEntity = new ThreadLocal<>();
		private final Function<SRC, SRCID> idProvider;
		
		public InMemoryRelationHolder(Function<SRC, SRCID> idProvider) {
			this.idProvider = idProvider;
		}
		
		@Override
		public void apply(SRC source, TRGT input) {
			Map<SRCID, C> srcidcMap = relationCollectionPerEntity.get();
			C collection = srcidcMap.computeIfAbsent(idProvider.apply(source), id -> getCollectionFactory().get());
			collection.add(input);
			// bidirectional assignment
			preventNull(getReverseSetter(), NOOP_REVERSE_SETTER).accept(input, source);
		}
		
		public C get(SRC src) {
			Map<SRCID, C> currentMap = relationCollectionPerEntity.get();
			return currentMap == null ? null : currentMap.get(idProvider.apply(src));
		}
		
		public void init() {
			this.relationCollectionPerEntity.set(new HashMap<>());
		}
		
		public void clear() {
			this.relationCollectionPerEntity.remove();
		}
	}
}
