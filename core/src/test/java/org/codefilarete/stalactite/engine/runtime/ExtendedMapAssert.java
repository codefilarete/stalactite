package org.codefilarete.stalactite.engine.runtime;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiPredicate;

import org.assertj.core.api.AbstractMapAssert;
import org.assertj.core.internal.ComparatorBasedComparisonStrategy;
import org.assertj.core.internal.IterableElementComparisonStrategy;
import org.assertj.core.internal.Objects;

/**
 * Since AssertJ {@link org.assertj.core.api.MapAssert} doesn't support {@link #usingElementComparator(Comparator)}
 * we have to implement it by ourselves : we do it through {@link #usingElementPredicate(BiPredicate)} and
 * {@link #containsExactlyInAnyOrder(Entry[])}
 *
 * @param <KEY>
 * @param <VALUE>
 * @author Guillaume Mary
 */
class ExtendedMapAssert<KEY, VALUE> extends AbstractMapAssert<ExtendedMapAssert<KEY, VALUE>, Map<KEY, VALUE>, KEY, VALUE> {
	
	protected org.assertj.core.internal.Iterables iterables = org.assertj.core.internal.Iterables.instance();
	
	public static <K, V> ExtendedMapAssert<K, V> assertThatMap(Map<K, V> actual) {
		return new ExtendedMapAssert<>(actual);
	}
	
	public ExtendedMapAssert(Map<KEY, VALUE> actual) {
		super(actual, ExtendedMapAssert.class);
	}
	
	@SafeVarargs
	public final ExtendedMapAssert<KEY, VALUE> containsExactlyInAnyOrder(Entry<? extends KEY, ? extends VALUE>... values) {
		return containsExactlyInAnyOrderForProxy(values);
	}
	
	protected ExtendedMapAssert<KEY, VALUE> containsExactlyInAnyOrderForProxy(Entry<? extends KEY, ? extends VALUE>[] values) {
		iterables.assertContainsExactlyInAnyOrder(info, actual.entrySet(), values);
		return myself;
	}
	
	public ExtendedMapAssert<KEY, VALUE> usingElementPredicate(BiPredicate<? super Entry<? extends KEY, ? extends VALUE>, ? super Entry<? extends KEY, ? extends VALUE>> predicate) {
		Comparator<? super Entry<? extends KEY, ? extends VALUE>> comparator = (o1, o2) -> predicate.test(o1, o2) ? 0 : -1;
		this.iterables = new org.assertj.core.internal.Iterables(new ComparatorBasedComparisonStrategy(comparator));
		// to have the same semantics on base assertions like isEqualTo, we need to use an iterable comparator comparing
		// elements with elementComparator parameter
		objects = new Objects(new IterableElementComparisonStrategy<>(comparator));
		return myself;
	}
}
