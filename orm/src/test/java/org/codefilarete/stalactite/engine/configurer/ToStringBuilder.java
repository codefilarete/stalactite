package org.codefilarete.stalactite.engine.configurer;

import java.util.function.Function;

import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;

public class ToStringBuilder<E> {
	
	@SafeVarargs
	public static <E> Function<E, String> of(String separator, Function<E, String>... properties) {
		ToStringBuilder<E> result = new ToStringBuilder<>(separator);
		for (Function<E, String> property : properties) {
			result.with(property);
		}
		return result::toString;
	}
	
	public static <E> Function<? extends Iterable<E>, String> asSeveral(Function<E, String> mapper) {
		return coll -> new StringAppender() {
			@Override
			public StringAppender cat(Object s) {
				return super.cat(s instanceof String ? s : mapper.apply((E) s));
			}
		}.ccat(coll, "").wrap("{", "}").toString();
	}
	
	private final String separator;
	private final KeepOrderSet<Function<E, String>> mappers = new KeepOrderSet<>();
	
	private ToStringBuilder(String separator) {
		this.separator = separator;
	}
	
	ToStringBuilder<E> with(Function<E, String> mapper) {
		this.mappers.add(mapper);
		return this;
	}
	
	String toString(E object) {
		return new StringAppender().ccat(Iterables.collectToList(mappers, m -> m.apply(object)), separator).toString();
	}
}
