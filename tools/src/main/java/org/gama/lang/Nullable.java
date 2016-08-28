package org.gama.lang;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.gama.lang.bean.IConverter;
import org.gama.lang.bean.IQuietConverter;

/**
 * @author Guillaume Mary
 */
public class Nullable<T> {
	
	public static <T> Nullable<T> of(T value) {
		return new Nullable<>(value);
	}
	
	private final T value;
	
	public Nullable() {
		this(null);
	}
	
	private Nullable(T value) {
		this.value = value;
	}
	
	public T get() {
		return value;
	}
	
	public T orGet(T anotherValue) {
		return isPresent() ? get() : anotherValue;
	}
	
	public boolean isPresent() {
		return value != null;
	}
	
	public <C> Nullable<C> orApply(Function<T, C> function) {
		return Nullable.of(doIfPresent(function));
	}
	
	public <C, E extends Exception> Nullable<C> orConvert(IConverter<T, C, E> function) throws E {
		return Nullable.of(doIfPresent(function));
	}
	
	public Nullable<Boolean> orTest(Predicate<T> predicate) {
		return Nullable.of(doIfPresent(ofPredicate(predicate)));
	}
	
	public void orAccept(Consumer<T> consumer) {
		doIfPresent(consumer);
	}
	
	private void doIfPresent(Consumer<T> consumer) {
		doIfPresent(new IQuietConverter<T, Void>() {
			@Override
			public Void convert(T input) {
				consumer.accept(input);
				return null;
			}
		});
	}
	
	private <O> O doIfPresent(Function<T, O> consumer) {
		return doIfPresent((IQuietConverter<T, O>) consumer::apply);
	}
	
	private <O, E extends Exception> O doIfPresent(IConverter<T, O, E> consumer) throws E {
		return isPresent() ? consumer.convert(get()) : null;
	}
	
	private static <T> Function<T, Boolean> ofPredicate(Predicate<T> predicate) {
		return predicate::test;
	}
}
