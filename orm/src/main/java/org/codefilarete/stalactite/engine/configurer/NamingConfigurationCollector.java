package org.codefilarete.stalactite.engine.configurer;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.MapEntryTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.UniqueConstraintNamingStrategy;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.tool.Nullable.empty;

/**
 * Build a {@link NamingConfiguration} from an {@link EntityMappingConfiguration} by iterating over its hierarchy
 * 
 * @author Guillaume Mary
 */
public class NamingConfigurationCollector {
	
	private final EntityMappingConfiguration<?, ?> entityMappingConfiguration;
	
	public NamingConfigurationCollector(EntityMappingConfiguration<?, ?> entityMappingConfiguration) {
		this.entityMappingConfiguration = entityMappingConfiguration;
	}
	
	public NamingConfiguration collect() {
		
		EntityPropertyCollector<TableNamingStrategy> tableNamingCollector = new EntityPropertyCollector<>(EntityMappingConfiguration::getTableNamingStrategy);
		// When a ColumnNamingStrategy is defined on mapping, it must be applied to super classes too
		EmbeddablePropertyCollector<ColumnNamingStrategy> columnNamingCollector = new EmbeddablePropertyCollector<>(EntityMappingConfiguration::getColumnNamingStrategy, EmbeddableMappingConfiguration::getColumnNamingStrategy);
		EntityPropertyCollector<ForeignKeyNamingStrategy> foreignKeyNamingCollector = new EntityPropertyCollector<>(EntityMappingConfiguration::getForeignKeyNamingStrategy);
		EmbeddablePropertyCollector<UniqueConstraintNamingStrategy> uniqueConstraintNamingCollector = new EmbeddablePropertyCollector<>(EntityMappingConfiguration::getUniqueConstraintNamingStrategy, EmbeddableMappingConfiguration::getUniqueConstraintNamingStrategy);
		EntityPropertyCollector<JoinColumnNamingStrategy> joinColumnNamingCollector = new EntityPropertyCollector<>(EntityMappingConfiguration::getJoinColumnNamingStrategy);
		EntityPropertyCollector<ColumnNamingStrategy> indexColumnNamingCollector = new EntityPropertyCollector<>(EntityMappingConfiguration::getIndexColumnNamingStrategy);
		EntityPropertyCollector<AssociationTableNamingStrategy> associationTableNamingCollector = new EntityPropertyCollector<>(EntityMappingConfiguration::getAssociationTableNamingStrategy);
		EntityPropertyCollector<ElementCollectionTableNamingStrategy> elementCollectionTableNamingCollector = new EntityPropertyCollector<>(EntityMappingConfiguration::getElementCollectionTableNamingStrategy);
		EntityPropertyCollector<MapEntryTableNamingStrategy> mapEntryTableNamingCollector = new EntityPropertyCollector<>(EntityMappingConfiguration::getEntryMapTableNamingStrategy);
		
		PropertiesCollector propertiesCollector = new PropertiesCollector();
		propertiesCollector.addEntityPropertyCollector(tableNamingCollector);
		propertiesCollector.addEmbeddablePropertyCollector(columnNamingCollector);
		propertiesCollector.addEntityPropertyCollector(foreignKeyNamingCollector);
		propertiesCollector.addEmbeddablePropertyCollector(uniqueConstraintNamingCollector);
		propertiesCollector.addEntityPropertyCollector(joinColumnNamingCollector);
		propertiesCollector.addEntityPropertyCollector(indexColumnNamingCollector);
		propertiesCollector.addEntityPropertyCollector(associationTableNamingCollector);
		propertiesCollector.addEntityPropertyCollector(elementCollectionTableNamingCollector);
		propertiesCollector.addEntityPropertyCollector(mapEntryTableNamingCollector);
		
		visitInheritedConfigurations(propertiesCollector);
		
		return new NamingConfiguration(
				tableNamingCollector.getResult().getOr(TableNamingStrategy.DEFAULT),
				columnNamingCollector.getResult().getOr(ColumnNamingStrategy.DEFAULT),
				foreignKeyNamingCollector.getResult().getOr(ForeignKeyNamingStrategy.DEFAULT),
				uniqueConstraintNamingCollector.getResult().getOr(UniqueConstraintNamingStrategy.DEFAULT),
				elementCollectionTableNamingCollector.getResult().getOr(ElementCollectionTableNamingStrategy.DEFAULT),
				mapEntryTableNamingCollector.getResult().getOr(MapEntryTableNamingStrategy.DEFAULT),
				joinColumnNamingCollector.getResult().getOr(JoinColumnNamingStrategy.JOIN_DEFAULT),
				indexColumnNamingCollector.getResult().getOr(ColumnNamingStrategy.INDEX_DEFAULT),
				associationTableNamingCollector.getResult().getOr(AssociationTableNamingStrategy.DEFAULT));
	}
	
	/**
	 * Visits parent {@link EntityMappingConfiguration}s of current entity mapping (including itself), this is an optional operation
	 * because current configuration may not have a direct entity ancestor.
	 * Then visits mapped super classes as {@link EmbeddableMappingConfiguration} of the last visited {@link EntityMappingConfiguration}, optional
	 * operation too.
	 * This is because inheritance can only have 2 paths :
	 * - inheritance from some other entities, then inheritance from some embeddable classes
	 * - inheritance from some embeddable classes
	 * This is because embeddable classes can't inherit from any entity (else it would be an embeddable with an identifier, which is an entity)
	 *
	 * @param collector
	 */
	void visitInheritedConfigurations(PropertiesCollector collector) {
		// iterating over mapping from inheritance
		Holder<EntityMappingConfiguration<?, ?>> lastMapping = new Holder<>();
		// iterating over inheritance mapping from bottom to top
		entityMappingConfiguration.inheritanceIterable().forEach(entityMappingConfiguration -> {
			collector.getEntityPropertyCollectors().forEach(entityPropertyCollector -> entityPropertyCollector.accept(entityMappingConfiguration));
			collector.getEmbeddablePropertyCollectors().forEach(embeddablePropertyCollector -> embeddablePropertyCollector.accept(entityMappingConfiguration));
			lastMapping.set(entityMappingConfiguration);
		});
		if (lastMapping.get().getPropertiesMapping().getMappedSuperClassConfiguration() != null) {
			// iterating over mapping from mapped super classes
			lastMapping.get().getPropertiesMapping().getMappedSuperClassConfiguration().inheritanceIterable().forEach(embeddableMappingConfiguration -> {
				collector.getEmbeddablePropertyCollectors().forEach(embeddablePropertyCollector -> embeddablePropertyCollector.accept(embeddableMappingConfiguration));
			});
		}
	}
	
	private static class PropertiesCollector {
		
		private final Set<EntityPropertyCollector<?>> entityPropertyCollectors = new HashSet<>();
		
		private final Set<EmbeddablePropertyCollector<?>> embeddablePropertyCollectors = new HashSet<>();
		
		PropertiesCollector() {
		}
		
		public <P> void addEntityPropertyCollector(EntityPropertyCollector<P> entityPropertyCollector) {
			this.entityPropertyCollectors.add(entityPropertyCollector);
		}
		
		public <P> void addEmbeddablePropertyCollector(EmbeddablePropertyCollector<P> embeddablePropertyCollector) {
			this.embeddablePropertyCollectors.add(embeddablePropertyCollector);
		}
		
		public Set<EntityPropertyCollector<?>> getEntityPropertyCollectors() {
			return entityPropertyCollectors;
		}
		
		public Set<EmbeddablePropertyCollector<?>> getEmbeddablePropertyCollectors() {
			return embeddablePropertyCollectors;
		}
	}
	
	/**
	 * Will collect a property on entity mappings
	 * 
	 * @param <P> the property type to collect
	 * @author Guillaume Mary
	 */
	private static class EntityPropertyCollector<P> implements Consumer<EntityMappingConfiguration> {
		
		private final Function<EntityMappingConfiguration, P> propertyGetter;
		
		private final Nullable<P> holder = empty();
		
		private EntityPropertyCollector(Function<EntityMappingConfiguration, P> propertyGetter) {
			this.propertyGetter = propertyGetter;
		}
		
		@Override
		public void accept(EntityMappingConfiguration entityMappingConfiguration) {
			P property = propertyGetter.apply(entityMappingConfiguration);
			holder.setIfAbsent(property);
		}
		
		public Nullable<P> getResult() {
			return holder;
		}
	}
	
	/**
	 * Will collect the same property on both entity and embeddable mappings
	 * 
	 * @param <P> the property type to collect
	 * @author Guillaume Mary
	 */
	private static class EmbeddablePropertyCollector<P> implements Consumer<EmbeddableMappingConfiguration> {
		
		private final EntityPropertyCollector<P> entityPropertyCollector;
		private final Function<EmbeddableMappingConfiguration, P> embeddablePropertyGetter;
		
		private EmbeddablePropertyCollector(Function<EntityMappingConfiguration, P> propertyGetter,
		                                    Function<EmbeddableMappingConfiguration, P> embeddablePropertyGetter) {
			this.entityPropertyCollector = new EntityPropertyCollector<>(propertyGetter);
			this.embeddablePropertyGetter = embeddablePropertyGetter;
		}
		
		public void accept(EntityMappingConfiguration embeddableMappingConfiguration) {
			entityPropertyCollector.accept(embeddableMappingConfiguration);
		}
		
		@Override
		public void accept(EmbeddableMappingConfiguration embeddableMappingConfiguration) {
			P property = embeddablePropertyGetter.apply(embeddableMappingConfiguration);
			entityPropertyCollector.getResult().setIfAbsent(property);
		}
		
		public Nullable<P> getResult() {
			return entityPropertyCollector.getResult();
		}
	}
}
