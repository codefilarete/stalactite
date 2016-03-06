package org.gama.lang.bean;

/**
 * @author Guillaume Mary
 */
public class Objects {
	
	/**
	 * Vérifie que o1.equals(o2) en prenant en compte les cas où o1 ou o2 sont null.
	 *
	 * @param o1 un objet
	 * @param o2 un objet
	 * @return true si o1 == null && o2 == null
	 *		<br> o1.equals(o2) si o1 et o2 non null
	 *		<br> false si o1 != null ou-exclusif o2 != null
	 */
	public static boolean equalsWithNull(Object o1, Object o2) {
		if (o1 == null && o2 == null) {
			return true;
		} else if (o1 != null ^ o2 != null) {
			return false;
		} else {
			return o1.equals(o2);
		}
	}
	
	public static <T, U> boolean equalsWithNull(T t, U u, BiPredicate<T, U> equalsNonNullDelegate) {
		if (t == null && u == null) {
			return true;
		} else if (t != null ^ u != null) {
			return false;
		} else {
			return equalsNonNullDelegate.test(t, u);
		}
	}
	
	public static <E> E preventNull(E value, E nullValue) {
		return value == null ? nullValue : value;
	}
	
	/** To be replaced by Java 8 Predicate */
	public interface Consumer<T> {
		void accept(T t);
	}
	
	/** To be replaced by Java 8 Predicate */
	public interface BiConsumer<T, U> {
		void accept(T t, U u);
	}
	
	/** To be replaced by Java 8 Predicate */
	public interface Predicate<T> {
		boolean test(T t);
	}
	
	/** To be replaced by Java 8 BiPredicate */
	public interface BiPredicate<T, U> {
		boolean test(T t, U u);
	
	}
	
	/** To be replaced by Java 8 Function */
	public interface Function<T, R> {
		R apply(T t);
	
	}
	
	/** To be replaced by Java 8 BiFunction */
	public interface BiFunction<T, U, R> {
		R apply(T t, U u);

	}
	
	/** To be replaced by Java 8 BiFunction */
	public interface Supplier<T> {
		T get();
	}
	
	/** To be replaced by Java 8 BiFunction */
	public static class Optional<T> {
		
		public static <T> Optional<T> of(T value) {
			if (value == null) throw new NullPointerException();
			return new Optional<>(value);
		}
		
		public static <T> Optional<T> ofNullable() {
			return new Optional<>();
		}
		
		private final T value;
		
		public Optional() {
			this(null);
		}
		
		private Optional(T value) {
			this.value = value;
		}
		
		public T get() {
			return value;
		}
		
		public T orElseGet(T anotherValue) {
			return isPresent() ? get() : anotherValue;
		}
		
		public boolean isPresent() {
			return value != null;
		}
		
		public void ifPresent(Consumer<? super T> consumer) {
			if (isPresent()) {
				consumer.accept(value);
			}
		}
	}
}
