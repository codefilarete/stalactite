package org.codefilarete.stalactite.engine.configurer.map;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.bean.Objects;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Made to store links between :
 * - source entity and [key-entity-id, value] pairs on one hand
 * - key-value records and key entity on the other hand
 * which let caller seam source entity and its [key entity, value] pairs afterward.
 * This is made necessary due to double join creation between
 * - source entity table and association table on one hand
 * - association table and key-entity table on one hand
 * Look at joinAsMany(..) invocations in {@link EntityAsValueMapRelationConfigurer#addSelectCascade(ConfiguredRelationalPersister, SimpleRelationalEntityPersister, PrimaryKey, ForeignKey, BiConsumer, Function, Supplier)}
 * This is the goal and need, implementation differ due to simplification made after first intent. 
 *
 * Expected to be used in a {@link SelectListener} to {@link #init()} it before select and {@link #clear()} it after select.
 *
 * @param <I>
 * @param <KEY_LOOKUP>
 * @param <ENTRY_VALUE>
 * @param <ENTITY>
 * @author Guillaume Mary
 */
class InMemoryRelationHolder<I, KEY_LOOKUP, ENTRY_VALUE, ENTITY> {
	
	public class Trio {
		private KEY_LOOKUP keyLookup;    // K or KID
		private ENTRY_VALUE entryValue;    // VID or KID
		private ENTITY entity;
		
		public KEY_LOOKUP getKeyLookup() {
			return keyLookup;
		}
		
		public ENTRY_VALUE getEntryValue() {
			return entryValue;
		}
		
		public ENTITY getEntity() {
			return entity;
		}
	}
	
	/**
	 * In memory and temporary Map storage.
	 */
	private final ThreadLocal<Map<I, Set<Trio>>> relationCollectionPerEntity = new ThreadLocal<>();

	private final Function<Trio, Duo<Object, Object>> mapper;
	
	public InMemoryRelationHolder(Function<Trio, Duo<Object, Object>> mapper) {
		this.mapper = mapper;
	}
	
	public void storeRelation(I source, KEY_LOOKUP keyLookup, ENTRY_VALUE entryValue) {
		Map<I, Set<Trio>> srcidcMap = relationCollectionPerEntity.get();
		Set<Trio> relatedDuos = srcidcMap.computeIfAbsent(source, id -> new HashSet<>());
		Trio trio = relatedDuos.stream().filter(pawn -> Objects.equals(pawn.keyLookup, keyLookup)).findAny().orElseGet(() -> {
			Trio result = new Trio();
			relatedDuos.add(result);
			return result;
		});
		trio.keyLookup = keyLookup;
		trio.entryValue = entryValue;
	}
	
	public void storeEntity(I source, KEY_LOOKUP keyLookup, ENTITY entity) {
		Map<I, Set<Trio>> srcidcMap = relationCollectionPerEntity.get();
		Set<Trio> relatedDuos = srcidcMap.computeIfAbsent(source, id -> new HashSet<>());
		// storeRelation(..) should have been invoked before this method (according to join building) so no need to build a Trio, null can be returned
		Trio trio = relatedDuos.stream().filter(pawn -> Objects.equals(pawn.keyLookup, keyLookup)).findAny().orElseGet(Trio::new);
		trio.entity = entity;
	}
	
	public Collection<Duo<Object, Object>> get(I src) {
		Map<I, Set<Trio>> currentMap = relationCollectionPerEntity.get();
		return nullable(currentMap)
				.map(map -> map.get(src))
				.map(map -> map.stream().map(mapper::apply)
						.collect(Collectors.toSet()))
				.get();
	}
	
	public void init() {
		this.relationCollectionPerEntity.set(new HashMap<>());
	}
	
	public void clear() {
		this.relationCollectionPerEntity.remove();
	}
}
