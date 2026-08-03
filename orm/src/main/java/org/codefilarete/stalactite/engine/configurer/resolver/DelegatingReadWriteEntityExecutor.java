package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.PersistExecutor;
import org.codefilarete.stalactite.engine.listener.DeleteByIdListener;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersistListener;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateByIdListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.tool.Duo;

public class DelegatingReadWriteEntityExecutor<C, I> implements EntityPersister<C, I> {
	
	private final EntityWriteExecutor<C, I> writer;
	private final ConfiguredEntityReader<C, I, ?> reader;
	private final PersistExecutor<C> persister;
	
	public DelegatingReadWriteEntityExecutor(EntityWriteExecutor<C, I> writer, ConfiguredEntityReader<C, I, ?> reader) {
		this.writer = writer;
		this.reader = reader;
		this.persister = PersistExecutor.forPersister(this, this);
	}
	
	public ConfiguredEntityReader<C, I, ?> getReader() {
		return reader;
	}
	
	@Override
	public Set<C> select(Iterable<I> ids) {
		return reader.select(ids);
	}
	
	@Override
	public ExecutableEntityQuery<C, ?> selectWhere() {
		return reader.selectWhere();
	}
	
	@Override
	public ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<SelectAdapter<C>> selectAdapter) {
		return reader.selectProjectionWhere(selectAdapter);
	}
	
	@Override
	public void addSelectListener(SelectListener<? extends C, I> selectListener) {
		reader.addSelectListener(selectListener);
	}
	
	@Override
	public void persist(Iterable<? extends C> entities) {
		persister.persist(entities);
	}
	
	@Override
	public <T extends Table<T>> EntityMapping<C, I, T> getMapping() {
		return writer.getMapping();
	}
	
	/**
	 * Select all instances with all relations fetched.
	 *
	 * @return all instance found in the database
	 */
	@Override
	public Set<C> selectAll() {
		return selectWhere().execute(Accumulators.toSet());
	}
	
	@Override
	public boolean isNew(C entity) {
		return writer.getMapping().isNew(entity);
	}
	
	@Override
	public I getId(C entity) {
		return writer.getId(entity);
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return writer.getMapping().getClassToPersist();
	}
	
	@Override
	public void delete(Iterable<? extends C> entities) {
		writer.delete(entities);
	}
	
	@Override
	public void deleteById(Iterable<? extends C> entities) {
		writer.deleteById(entities);
	}
	
	@Override
	public void insert(Iterable<? extends C> entities) {
		writer.insert(entities);
	}
	
	@Override
	public void updateById(Iterable<? extends C> entities) {
		writer.updateById(entities);
	}
	
	@Override
	public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		writer.update(differencesIterable, allColumnsStatement);
	}
	
	@Override
	public void addPersistListener(PersistListener<? extends C> persistListener) {
		writer.addPersistListener(persistListener);
	}
	
	@Override
	public void addInsertListener(InsertListener<? extends C> insertListener) {
		writer.addInsertListener(insertListener);
	}
	
	@Override
	public void addUpdateListener(UpdateListener<? extends C> updateListener) {
		writer.addUpdateListener(updateListener);
	}
	
	@Override
	public void addUpdateByIdListener(UpdateByIdListener<? extends C> updateByIdListener) {
		writer.addUpdateByIdListener(updateByIdListener);
	}
	
	@Override
	public void addDeleteListener(DeleteListener<? extends C> deleteListener) {
		writer.addDeleteListener(deleteListener);
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener<? extends C> deleteListener) {
		writer.addDeleteByIdListener(deleteListener);
	}
}
