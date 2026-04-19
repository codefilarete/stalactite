package org.codefilarete.stalactite.engine.configurer.model;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * Each subclass has its own dedicated table that joins to the parent
 * table via a FK on the subclass table pointing to the parent PK.
 *
 * @param <C>  parent entity type
 * @param <I>  identifier type
 * @param <T>  parent table type
 */
public class JoinTablePolymorphism<C, I, T extends Table<T>> implements EntityPolymorphism<C, I> {

    /** Sub-entities with their own tables, keyed by subclass type */
    private final Map<Class<? extends C>, JoinedSubEntity<? extends C, ?>> subEntities = new KeepOrderMap<>();
	
	public <D extends C, SUBTABLE extends Table<SUBTABLE>> void addSubEntity(
			Class<D> subType,
			Mapping<D, SUBTABLE> subEntity, DirectRelationJoin<T, SUBTABLE, I> join) {
		subEntities.put(subType, new JoinedSubEntity<>(subEntity, join));
	}
	
	@Override
	public Set<Mapping<? extends C, ?>> getSubEntities() {
		return subEntities.values().stream().map(JoinedSubEntity::getSubEntity).collect(Collectors.toCollection(KeepOrderSet::new));
	}
	
	/**
	 * A sub-entity in join-table polymorphism. It holds its own table and the FK join back to the parent.
	 * 
	 * @param <D> the subclass entity type
	 * @param <SUBTABLE> the subclass-specific table type
	 */
	public class JoinedSubEntity<D extends C, SUBTABLE extends Table<SUBTABLE>> {
	
		private final Mapping<D, SUBTABLE> subEntity;
	
		/** Join: subTable.FK → parentTable.PK */
		private final DirectRelationJoin<T, SUBTABLE, I> join;
		
		public JoinedSubEntity(Mapping<D, SUBTABLE> subEntity, DirectRelationJoin<T, SUBTABLE, I> join) {
			this.subEntity = subEntity;
			this.join = join;
		}
		
		public Mapping<D, SUBTABLE> getSubEntity() {
			return subEntity;
		}
		
		public DirectRelationJoin<T, SUBTABLE, I> getJoin() {
			return join;
		}
	}
}
