package org.codefilarete.stalactite.engine.configurer;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.IndexNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.MapEntryTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.manyToOne.ManyToOneRelation;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.map.MapRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * A {@link EntityMappingConfiguration} with a determined target {@link Table}.
 * Used when initial configuration has a null one and configurers has determined the target table (by reverse column, inheritance, etc.).
 * We could have also used {@link org.codefilarete.stalactite.engine.configurer.builder.DefaultPersisterBuilder#build(EntityMappingConfigurationProvider)}
 * with an extra table argument, but it complexifies the Builder Pattern.
 *
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public class EntityMappingConfigurationWithTable<C, I> implements EntityMappingConfiguration<C, I> {
	
	private final EntityMappingConfiguration<C, I> delegate;
	private final Table table;
	
	public EntityMappingConfigurationWithTable(EntityMappingConfiguration<C, I> delegate, Table table) {
		this.delegate = delegate;
		this.table = table;
	}
	
	@Nullable
	public EntityFactoryProvider<C, Table> getEntityFactoryProvider() {
		return delegate.getEntityFactoryProvider();
	}
	
	@Nullable
	public Table getTable() {
		return table;
	}
	
	@Nullable
	public TableNamingStrategy getTableNamingStrategy() {
		return delegate.getTableNamingStrategy();
	}
	
	public Iterable<EntityMappingConfiguration<? super C, I>> inheritanceIterable() {
		return delegate.inheritanceIterable();
	}
	
	public PolymorphismPolicy<C> getPolymorphismPolicy() {
		return delegate.getPolymorphismPolicy();
	}
	
	@Nullable
	public ColumnNamingStrategy getColumnNamingStrategy() {
		return delegate.getColumnNamingStrategy();
	}
	
	public KeyMapping<C, I> getKeyMapping() {
		return delegate.getKeyMapping();
	}
	
	public EmbeddableMappingConfiguration<C> getPropertiesMapping() {
		return delegate.getPropertiesMapping();
	}
	
	@Nullable
	public VersioningStrategy getOptimisticLockOption() {
		return delegate.getOptimisticLockOption();
	}
	
	@Nullable
	public MapEntryTableNamingStrategy getEntryMapTableNamingStrategy() {
		return delegate.getEntryMapTableNamingStrategy();
	}
	
	@Nullable
	public InheritanceConfiguration<? super C, I> getInheritanceConfiguration() {
		return delegate.getInheritanceConfiguration();
	}
	
	@Nullable
	public ColumnNamingStrategy getIndexColumnNamingStrategy() {
		return delegate.getIndexColumnNamingStrategy();
	}
	
	@Nullable
	public ForeignKeyNamingStrategy getForeignKeyNamingStrategy() {
		return delegate.getForeignKeyNamingStrategy();
	}
	
	@Override
	public IndexNamingStrategy getIndexNamingStrategy() {
		return delegate.getIndexNamingStrategy();
	}
	
	@Nullable
	public AssociationTableNamingStrategy getAssociationTableNamingStrategy() {
		return delegate.getAssociationTableNamingStrategy();
	}
	
	@Nullable
	public ElementCollectionTableNamingStrategy getElementCollectionTableNamingStrategy() {
		return delegate.getElementCollectionTableNamingStrategy();
	}
	
	@Nullable
	public JoinColumnNamingStrategy getJoinColumnNamingStrategy() {
		return delegate.getJoinColumnNamingStrategy();
	}
	
	public Class<C> getEntityType() {
		return delegate.getEntityType();
	}
	
	public <TRGT, TRGTID> List<OneToOneRelation<C, TRGT, TRGTID>> getOneToOnes() {
		return delegate.getOneToOnes();
	}
	
	public <TRGT, TRGTID> List<OneToManyRelation<C, TRGT, TRGTID, Collection<TRGT>>> getOneToManys() {
		return delegate.getOneToManys();
	}
	
	public <TRGT, TRGTID> List<ManyToManyRelation<C, TRGT, TRGTID, Collection<TRGT>, Collection<C>>> getManyToManys() {
		return delegate.getManyToManys();
	}
	
	public <TRGT, TRGTID> List<ManyToOneRelation<C, TRGT, TRGTID, Collection<C>>> getManyToOnes() {
		return delegate.getManyToOnes();
	}
	
	public <TRGT> List<ElementCollectionRelation<C, TRGT, ? extends Collection<TRGT>>> getElementCollections() {
		return delegate.getElementCollections();
	}
	
	public List<MapRelation<C, ?, ?, ? extends Map>> getMaps() {
		return delegate.getMaps();
	}
}
