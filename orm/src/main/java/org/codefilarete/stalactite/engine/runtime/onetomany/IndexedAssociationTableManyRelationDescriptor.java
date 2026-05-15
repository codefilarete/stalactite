package org.codefilarete.stalactite.engine.runtime.onetomany;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.codefilarete.reflection.PropertyMutator;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * Container to store information of an indexed *-to-many with association table relation (by a column on the association table)
 *
 * @author Guillaume Mary
 */
public class IndexedAssociationTableManyRelationDescriptor<SRC, TRGT, S extends Collection<TRGT>, SRCID> extends ManyRelationDescriptor<SRC, TRGT, S> {
	
	/**
	 * @param collectionAccessPoint collection accessor
	 * @param collectionFactory collection factory
	 * @param reverseSetter
	 */
	public IndexedAssociationTableManyRelationDescriptor(ReadWritePropertyAccessPoint<SRC, S> collectionAccessPoint,
														 Supplier<S> collectionFactory,
														 @Nullable PropertyMutator<TRGT, SRC> reverseSetter,
														 Function<SRC, SRCID> idProvider,
														 boolean maintainAssociationOnly,
														 boolean orphanRemoval) {
		super(collectionAccessPoint, collectionFactory, reverseSetter, maintainAssociationOnly, orphanRemoval);
		super.relationFixer = new InMemoryRelationHolder<>(idProvider, this);
	}
	
	@Override
	public InMemoryRelationHolder<SRC, SRCID, TRGT, S> getRelationFixer() {
		return (InMemoryRelationHolder) super.getRelationFixer();
	}
	
	/**
	 * A relation fixer that doesn't set the Collection onto the source bean but keeps it in memory.
	 * Made for collection with persisted order: it is sorted later with {@link #applySort(Set)} (to be called by some afterSelect(..) code),
	 * and set it onto source bean (by collection setter).
	 * Collection is kept in memory thanks to a ThreadLocal because its lifetime is expected to be a select execution,
	 * hence caller is expected to initialize and clean this instance by calling {@link #init()} and {@link #clear()}
	 * before and after selection.
	 *
	 * @see #applySort(Set)
	 * @author Guillaume Mary
	 */
	public static class InMemoryRelationHolder<SRC, SRCID, TRGT, S extends Collection<TRGT>> implements BeanRelationFixer<SRC, TRGT> {
		
		private class CollectionOrderStorage {
			
			// Note that we use index as key instead of target entities to allow them to appear twice in the same collection (List),
			// Moreover, thanks to this form, it's easy to be applied to final Collection because values() automatically returns the entities as sorted  
			
			private final Map<Integer /* index */, TRGT> targetPerIndex = new HashMap<>();
		}
		/**
		 * Context for indexed collections (List, LinkedHashSet, ...). It will keep the entity index during select between "unrelated" methods/phases:
		 * the index column must be added to the SQL select, read from ResultSet and then, the order is applied to sort the final List, but this
		 * particular feature crosses over layers (entities and SQL) which is not implemented. In such circumstances, ThreadLocal comes to the rescue.
		 * It could be static, but it would lack the TRGTID typing, which leads to some generics errors, so we left it non-static (which is an
		 * acceptable small overhead)
		 */
		// Note that we prefer to store Map<SRCID> instead of IdentityMap for now because it's simpler but requires identifier providers.
		// Could be replaced by IdentityMap<SRC>
		private final ThreadLocal<Map<SRCID, CollectionOrderStorage>> currentSelectedIndexes = new ThreadLocal<>();
		
		private final Function<SRC, SRCID> idProvider;
		
		private final ReadWritePropertyAccessPoint<SRC, S> collectionAccessPoint;
		
		private final Supplier<S> collectionFactory;
		
		private final PropertyMutator<TRGT, SRC> reverseSetter;
		
		public InMemoryRelationHolder(Function<SRC, SRCID> idProvider, IndexedAssociationTableManyRelationDescriptor<SRC, TRGT, S, SRCID> manyRelationDescriptor) {
			this(idProvider, manyRelationDescriptor.getCollectionAccessPoint(), manyRelationDescriptor.getCollectionFactory(), manyRelationDescriptor.getReverseSetter());
		}
		
		public InMemoryRelationHolder(Function<SRC, SRCID> idProvider,
		                              ReadWritePropertyAccessPoint<SRC, S> collectionAccessPoint,
		                              Supplier<S> collectionFactory,
		                              PropertyMutator<TRGT, SRC> reverseSetter) {
			this.idProvider = idProvider;
			this.collectionAccessPoint = collectionAccessPoint;
			this.collectionFactory = collectionFactory;
			this.reverseSetter = (PropertyMutator<TRGT, SRC>) preventNull(reverseSetter, NOOP_REVERSE_SETTER);;
		}
		
		public void addIndex(SRCID leftEntityId, TRGT trgt, int index) {
			currentSelectedIndexes.get().get(leftEntityId).targetPerIndex.put(index, trgt);
		}
		
		@Override
		public void apply(SRC source, TRGT input) {
			// we store the relation in memory without setting it onto source entity because we need to sort it later
			currentSelectedIndexes.get().computeIfAbsent(idProvider.apply(source), srcid -> new CollectionOrderStorage());
			// bidirectional assignment
			reverseSetter.set(input, source);
		}
		
		public void init() {
			this.currentSelectedIndexes.set(new HashMap<>());
		}
		
		public void clear() {
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
				CollectionOrderStorage inMemoryCollection = currentSelectedIndexes.get().get(idProvider.apply(src));
				if (inMemoryCollection != null) {  // inMemoryCollection can be null if there's no associated entity in the database
					Map<Integer, TRGT> targetIdPerId = inMemoryCollection.targetPerIndex;
					S relationCollection = collectionAccessPoint.get(src);
					if (relationCollection == null) {
						relationCollection = collectionFactory.get();
						collectionAccessPoint.set(src, relationCollection);
					}
					relationCollection.addAll(new LinkedList<>(targetIdPerId.values()));
				}
			});
		}
	}
}
