package org.codefilarete.stalactite.sql.result;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.tool.function.Hanger.Holder;

/**
 * A library of {@link Accumulator}
 * 
 * @author Guillaume Mary
 */
public final class Accumulators {
	
	/**
	 * Creates an {@link Accumulator} that adds input elements into a new {@link Collection} (created by given factory).
	 * 
	 * @param <T> type of elements
	 * @param <C> resulting {@link Collection} type
	 * @param collectionFactory a {@link Supplier} returning a new {@link Collection} of appropriate type
	 * @return an {@link Accumulator} which collects all the input elements into a {@link Collection}, in encounter order
	 */
	public static <T, C extends Collection<T>> Accumulator<T, C, C> toCollection(Supplier<C> collectionFactory) {
		return new CollectionSupport<>(collectionFactory);
	}
	
	/**
	 * Creates an {@link Accumulator} that adds input elements into a new {@link List}.
	 * 
	 * @param <T> type of input elements
	 * @return an {@link Accumulator} which collects all the input elements into a {@code List}, in encounter order
	 * @see #toUnmodifiableList() 
	 */
	public static <T> Accumulator<T, List<T>, List<T>> toList() {
		return new CollectionSupport<>(ArrayList::new);
	}
	
	/**
	 * Creates an {@link Accumulator} that adds input elements into a new unmodifiable {@link List}.
	 * 
	 * @param <T> type of input elements
	 * @return an {@link Accumulator} which collects all the input elements into a {@link List}, in encounter order
	 */
	public static <T> Accumulator<T, List<T>, List<T>> toUnmodifiableList() {
		return Accumulators.<T>toList().andThen(Collections::unmodifiableList);
	}
	
	/**
	 * Creates an {@link Accumulator} that adds input elements into a new {@link Set}.
	 * 
	 * @param <T> type of input elements
	 * @return an {@link Accumulator} which collects all the input elements into a {@link Set}, in encounter order
	 */
	public static <T> Accumulator<T, Set<T>, Set<T>> toSet() {
		return new CollectionSupport<>(HashSet::new);
	}
	
	/**
	 * Creates an {@link Accumulator} that adds input elements into a new unmodifiable {@link Set}.
	 * 
	 * @param <T> type of input elements
	 * @return an {@link Accumulator} which collects all the input elements into a {@link Set}, in encounter order
	 */
	public static <T> Accumulator<T, Set<T>, Set<T>> toUnmodifiableSet() {
		return Accumulators.<T>toSet().andThen(Collections::unmodifiableSet);
	}
	
	/**
	 * Creates an {@link Accumulator} that adds input elements into a new {@link Set} and keep encounter order.
	 * Expected use is for query which is already sorted : it avoids to lose sorting order, as a difference with {@link #toSet()}.
	 *
	 * @param <T> type of input elements
	 * @return an {@link Accumulator} which collects all the input elements into a {@link Set}, in encounter order
	 */
	public static <T> Accumulator<T, Set<T>, Set<T>> toKeepingOrderSet() {
		return new CollectionSupport<>(LinkedHashSet::new);
	}
	
	/**
	 * Creates an {@link Accumulator} that adds input elements into a new {@link NavigableSet}.
	 *
	 * @param comparator the {@link Comparator} used to compare input elements
	 * @param <T> type of input elements
	 * @return an {@link Accumulator} which collects all the input elements into a {@link Set}, in encounter order
	 */
	public static <T> Accumulator<T, NavigableSet<T>, NavigableSet<T>> toSortedSet(Comparator<T> comparator) {
		return new CollectionSupport<>(() -> new TreeSet<>(comparator));
	}
	
	/**
	 * Creates an {@link Accumulator} that puts input elements into a new {@link Map} as value with key given by
	 * {@code keyMapper}.
	 * If key is encountered several times, only first element is kept in final result.
	 * 
	 * @param <T> type of input elements
	 * @param <K> type of {@link Map} key
	 * @return an {@link Accumulator} which collects all the input elements into a {@link Set}, in encounter order
	 */
	public static <T, K> Accumulator<T, ?, Map<K, T>> groupingBy(Function<? super T, ? extends K> keyMapper) {
		return groupingBy(keyMapper, HashMap::new, getFirst());
	}
	
	/**
	 * Creates an {@link Accumulator} that puts input elements into a new {@link Map} with key given by
	 * {@code keyMapper} and value given by {@code downstream}.
	 * If key is encountered several times, only first element is kept in final result.
	 * 
	 * @param <T> type of input elements
	 * @param <K> type of {@link Map} key
	 * @param <V> type of {@link Map} value
	 * @return an {@link Accumulator} which collects all the input elements into a {@link Set}, in encounter order
	 */
	public static <T, K, V> Accumulator<T, ?, Map<K, V>> groupingBy(
			Function<? super T, ? extends K> keyMapper,
			Accumulator<? super T, ?, V> downstream) {
		return groupingBy(keyMapper, HashMap::new, downstream);
	}
	
	/**
	 * Creates an {@link Accumulator} that puts input elements into a new {@link Map} with key given by
	 * {@code keyMapper} and value given by {@code downstream}.
	 * {@link Map} instance creation is controlled by {@code mapFactory}.
	 * 
	 * @param <T> type of input elements
	 * @param <K> type of {@link Map} key
	 * @param <V> type of {@link Map} value
	 * @param <M> type of resulting {@link Map}
	 * @param <S> type of {@code downStream} seed
	 * @return an {@link Accumulator} which collects all the input elements into a {@link Set}, in encounter order
	 */
	public static <T, K, V, S, M extends Map<K, V>> Accumulator<T, ?, M> groupingBy(
			Function<? super T, ? extends K> keyMapper,
			Supplier<M> mapFactory,
			Accumulator<? super T, S, V> downstream) {
		// Algorithm : resulting Map is built by mapFactory and aggregation is made by downstream one, which puts object
		// of wrong type in Map, which is doable thanks to Map plasticity, finally all values are replaced by downstream
		// finisher with appropriate type
		Supplier<S> downstreamSupplier = downstream.supplier();
		BiConsumer<S, ? super T> downstreamAggregator = downstream.aggregator();
		BiConsumer<Map<K, S>, T> accumulator = (m, t) -> {
			K key = keyMapper.apply(t);
			S container = m.computeIfAbsent(key, k -> downstreamSupplier.get());
			downstreamAggregator.accept(container, t);
		};
		// Downstream finisher will replace every value by its own type in resulting map
		Function<S, S> downstreamFinisher = (Function<S, S>) downstream.finisher();
		Function<Map<K, S>, M> finisher = resultingMap -> {
			resultingMap.replaceAll((k, v) -> downstreamFinisher.apply(v));
			return (M) resultingMap;
		};
		return new AccumulatorSupport<>((Supplier<Map<K, S>>) mapFactory, accumulator, finisher);
	}
	
	/**
	 * Creates an {@link Accumulator} that extracts a property from input elements and aggregates it into another {@link Accumulator}.
	 *
	 * @param <T> type of input elements
	 * @param <K> type of extracting property
	 * @param <S> type of the seed of the secondary {@link Accumulator}
	 * @param <R> type of the result
	 * @return an {@link Accumulator} which collects all the input elements into a {@link Set}, in encounter order
	 */
	public static <T, K, S, R> Accumulator<T, ?, R> mapping(
			Function<? super T, ? extends K> mapper,
			Accumulator<? super K, S, R> downstream) {
		BiConsumer<S, ? super K> downstreamAccumulator = downstream.aggregator();
		return new AccumulatorSupport<>(downstream.supplier(),
				(r, t) -> downstreamAccumulator.accept(r, mapper.apply(t)),
				downstream.finisher());
	}
	
	/**
	 * Creates an {@link Accumulator} that returns first non-null input element (will return null if no non-null
	 * elements were found).
	 * <strong>It will return first element / bean / entity, not row. Meaning that whole {@link java.sql.ResultSet} will
	 * be consumed to build the bean.</strong>
	 *
	 * @param <T> type of input elements
	 * @return an {@link Accumulator} which returns first non-null input element
	 */
	public static <T> Accumulator<T, ?, T> getFirst() {
		return new AccumulatorSupport<T, Holder<T>, T>(Holder::new, (holder, t) -> {
			if (holder.get() == null) {
				holder.set(t);
			}
		}, Holder::get);
	}
	
	/**
	 * Creates an {@link Accumulator} that returns first non-null input element which is expected to be the only one :
	 * will throw an exception if another non-null object is found
	 *
	 * @param <T> type of input elements
	 * @return an {@link Accumulator} which returns last input element
	 */
	public static <T> Accumulator<T, ?, T> getFirstUnique() {
		return new AccumulatorSupport<T, Holder<T>, T>(Holder::new, (holder, t) -> {
			if (holder.get() != null) {
				throw new NonUniqueObjectException("Object was expected to be a lonely one but another object is already present");
			}
			holder.set(t);
		}, Holder::get);
	}
	
	static class CollectionSupport<T, C extends Collection<T>> extends AccumulatorSupport<T, C, C> {
		
		CollectionSupport(Supplier<C> supplier) {
			super(supplier, Collection::add);
		}
	}
	
	/**
	 * Generic {@link Accumulator} implementation class.
	 *
	 * @param <T> the type of elements to be collected
	 * @param <S> the type of the seed that collects elements
	 * @param <R> the type of the result
	 */
	static class AccumulatorSupport<T, S, R> implements Accumulator<T, S, R> {
		private final Supplier<S> supplier;
		private final BiConsumer<S, T> accumulator;
		private final Function<S, R> finisher;
		
		AccumulatorSupport(Supplier<S> supplier,
						   BiConsumer<S, T> accumulator,
						   Function<S, R> finisher) {
			this.supplier = supplier;
			this.accumulator = accumulator;
			this.finisher = finisher;
		}
		
		AccumulatorSupport(Supplier<S> supplier,
						   BiConsumer<S, T> accumulator) {
			this(supplier, accumulator, s -> (R) s);
		}
		
		@Override
		public Supplier<S> supplier() {
			return this.supplier;
		}
		
		@Override
		public BiConsumer<S, T> aggregator() {
			return this.accumulator;
		}
		
		@Override
		public Function<S, R> finisher() {
			return this.finisher;
		}
	}
	
	public static class NonUniqueObjectException extends RuntimeException {
		
		public NonUniqueObjectException(String message) {
			super(message);
		}
	}
	
	/** Not-exposed constructor to prevent from instantiation to apply tool class design */
	private Accumulators() {
		// voluntarily empty
	}
}