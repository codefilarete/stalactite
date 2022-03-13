package org.codefilarete.stalactite.engine.cascade;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Predicates;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;

/**
 * @author Guillaume Mary
 */
public class BeforeDeleteByIdSupport<TRIGGER, TARGET> implements DeleteByIdListener<TRIGGER> {
	
	private final Consumer<Iterable<TARGET>> beforeDeleteAction;
	
	private final Function<TRIGGER, TARGET> targetProvider;
	
	private final Predicate<TARGET> targetFilter;
	
	public BeforeDeleteByIdSupport(Consumer<Iterable<TARGET>> beforeDeleteAction, Function<TRIGGER, TARGET> targetProvider) {
		this(beforeDeleteAction, targetProvider, Predicates.acceptAll());
	}
	
	public BeforeDeleteByIdSupport(Consumer<Iterable<TARGET>> beforeDeleteAction, Function<TRIGGER, TARGET> targetProvider, Predicate<TARGET> targetFilter) {
		this.beforeDeleteAction = beforeDeleteAction;
		this.targetProvider = targetProvider;
		this.targetFilter = targetFilter;
	}

	@Override
	public void beforeDeleteById(Iterable<TRIGGER> entities) {
		beforeDeleteAction.accept(Iterables.stream(entities).map(targetProvider).filter(targetFilter).collect(Collectors.toList()));
	}
}
