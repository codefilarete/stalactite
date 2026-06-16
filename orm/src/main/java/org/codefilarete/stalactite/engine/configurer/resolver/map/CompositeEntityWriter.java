package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Collection;
import java.util.Map;

import org.codefilarete.stalactite.engine.runtime.CollectionUpdater.EntityWriter;
import org.codefilarete.tool.Duo;

/**
 * {@link EntityWriter} delegating every write operation to a list of {@link EntityWriter}, in order. Used to cascade
 * to both the key and the value persisters when both sides of the {@link Map} are entities.
 *
 * @param <C> managed element type (here a {@link Map.Entry})
 * @author Guillaume Mary
 */
class CompositeEntityWriter<C> implements EntityWriter<C, Object> {
	
	private final Collection<? extends EntityWriter<C, ?>> delegates;
	
	CompositeEntityWriter(Collection<? extends EntityWriter<C, ?>> delegates) {
		this.delegates = delegates;
	}
	
	@Override
	public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		delegates.forEach(writer -> writer.update(differencesIterable, allColumnsStatement));
	}
	
	@Override
	public void delete(Iterable<? extends C> entities) {
		delegates.forEach(writer -> writer.delete(entities));
	}
	
	@Override
	public void insert(Iterable<? extends C> entities) {
		delegates.forEach(writer -> writer.insert(entities));
	}
	
	@Override
	public void persist(Iterable<? extends C> entities) {
		delegates.forEach(writer -> writer.persist(entities));
	}
	
	@Override
	public void updateById(Iterable<? extends C> entities) {
		delegates.forEach(writer -> writer.updateById(entities));
	}
}
