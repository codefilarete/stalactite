package org.codefilarete.stalactite.engine.cascade;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.listener.UpdateByIdListener;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Predicates;

/**
 * @author Guillaume Mary
 */
public class AfterUpdateByIdSupport<TRIGGER, TARGET> implements UpdateByIdListener<TRIGGER> {
	
	private final Consumer<Iterable<TARGET>> afterUpdateAction;
	
	private final Function<TRIGGER, TARGET> targetProvider;
	
	private final Predicate<TARGET> targetFilter;
	
	public AfterUpdateByIdSupport(Consumer<Iterable<TARGET>> afterUpdateAction, Function<TRIGGER, TARGET> targetProvider) {
		this(afterUpdateAction, targetProvider, Predicates.acceptAll());
	}
	
	public AfterUpdateByIdSupport(Consumer<Iterable<TARGET>> afterUpdateAction, Function<TRIGGER, TARGET> targetProvider, Predicate<TARGET> targetFilter) {
		this.afterUpdateAction = afterUpdateAction;
		this.targetProvider = targetProvider;
		this.targetFilter = targetFilter;
	}
	
	@Override
	public void afterUpdateById(Iterable<? extends TRIGGER> entities) {
		afterUpdateAction.accept(Iterables.stream(entities).map(targetProvider).filter(targetFilter).collect(Collectors.toList()));
	}
}
