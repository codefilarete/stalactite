package org.codefilarete.stalactite.engine.configurer.model;

import java.util.Set;
import javax.annotation.Nullable;

import org.codefilarete.reflection.PropertyMutator;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.Serie;

public class Entity<C, I, T extends Table<T>> {
	
	private final IdentifierMapping<C, I> identifierMapping;
	
	@Nullable
	private Versioning<C, ?, T> versioning;
	
	private final Mapping<C, T> mapping;
	
	/**
	 * Parent entity of this one, from a mapped-superclass perspective (not as polymorphic one)
	 */
	private AncestorJoin<? super C, T, ?, I> parent;
	
	@Nullable
	private EntityPolymorphism<C, I> polymorphism;
	
	public Entity(IdentifierMapping<C, I> identifierMapping, Mapping<C, T> mapping) {
		this.mapping = mapping;
		this.identifierMapping = identifierMapping;
	}
	
	public IdentifierMapping<C, I> getIdentifierMapping() {
		return identifierMapping;
	}
	
	public ReadWritePropertyAccessPoint<C, I> getIdAccessor() {
		return identifierMapping.getIdAccessor();
	}
	
	@Nullable
	public Versioning<C, ?, T> getVersioning() {
		return versioning;
	}
	
	public void setVersioning(@Nullable Versioning<C, ?, T> versioning) {
		this.versioning = versioning;
	}
	
	public Mapping<C, T> getMapping() {
		return mapping;
	}
	
	public Class<C> getEntityType() {
		return mapping.getEntityType();
	}
	
	public T getTable() {
		return mapping.getTable();
	}
	
	public PropertyMappingHolder<C, T> getPropertyMappingHolder() {
		return mapping.getPropertyMappingHolder();
	}
	
	public Set<MappingJoin<?, ?, ?>> getRelations() {
		return mapping.getRelations();
	}
	
	public void addRelation(MappingJoin<?, ?, ?> relation) {
		mapping.addRelation(relation);
	}
	
	public AncestorJoin<? super C, T, ?, I> getParent() {
		return parent;
	}
	
	public void setParent(AncestorJoin<? super C, T, ?, I> parent) {
		this.parent = parent;
	}
	
	public void setPolymorphism(@Nullable EntityPolymorphism<C, I> polymorphism) {
		this.polymorphism = polymorphism;
	}
	
	@Nullable
	public EntityPolymorphism<C, I> getPolymorphism() {
		return polymorphism;
	}
	
	public static abstract class AbstractPropertyMapping<C, O, T extends Table<T>> {
		
		private final PropertyMutator<C, O> accessor;
		
		private final Column<T, O> column;
		
		@Nullable
		private final Converter<?, O> readConverter;
		
		private final boolean setByConstructor;
		
		private final boolean unique;
		
		public AbstractPropertyMapping(PropertyMutator<C, O> accessor, Column<T, O> column, boolean setByConstructor, @Nullable Converter<?, O> readConverter, boolean unique) {
			this.accessor = accessor;
			this.column = column;
			this.readConverter = readConverter;
			this.setByConstructor = setByConstructor;
			this.unique = unique;
		}
		
		public PropertyMutator<C, O> getAccessPoint() {
			return accessor;
		}
		
		public Column<T, O> getColumn() {
			return column;
		}
		
		@Nullable
		public Converter<?, O> getReadConverter() {
			return readConverter;
		}
		
		public boolean isSetByConstructor() {
			return setByConstructor;
		}
		
		public boolean isUnique() {
			return unique;
		}
	}
	
	public static class PropertyMapping<C, O, T extends Table<T>> extends AbstractPropertyMapping<C, O, T> {
		
		@Nullable
		private final Converter<O, ?> writeConverter;
		
		public PropertyMapping(ReadWritePropertyAccessPoint<C, O> accessor,
		                       Column<T, O> column,
		                       boolean setByConstructor,
		                       @Nullable Converter<?, O> readConverter,
		                       @Nullable Converter<O, ?> writeConverter,
		                       boolean unique) {
			super(accessor, column, setByConstructor, readConverter, unique);
			this.writeConverter = writeConverter;
		}
		
		@Nullable
		public Converter<O, ?> getWriteConverter() {
			return writeConverter;
		}
		
		@Override
		public ReadWritePropertyAccessPoint<C, O> getAccessPoint() {
			return (ReadWritePropertyAccessPoint<C, O>) super.getAccessPoint();
		}
	}
	
	public static class ReadOnlyPropertyMapping<C, O, T extends Table<T>> extends AbstractPropertyMapping<C, O, T> {
		
		public ReadOnlyPropertyMapping(PropertyMutator<C, O> accessor,
		                               Column<T, O> column,
		                               boolean setByConstructor,
		                               @Nullable Converter<?, O> readConverter,
		                               boolean unique) {
			super(accessor, column, setByConstructor, readConverter, unique);
		}
	}
	
	public static class Versioning<C, V, T extends Table<T>> {
		
		private final ReadWritePropertyAccessPoint<C, V> versioningAccessor;
		
		private final Column<T, V> versioningColumn;
		
		private final Serie<V> versioningSerie;
		
		public Versioning(ReadWritePropertyAccessPoint<C, V> versioningAccessor, Column<T, V> versioningColumn, Serie<V> versioningSerie) {
			this.versioningAccessor = versioningAccessor;
			this.versioningColumn = versioningColumn;
			this.versioningSerie = versioningSerie;
		}
		
		public ReadWritePropertyAccessPoint<C, V> getVersioningAccessor() {
			return versioningAccessor;
		}
		
		public Column<T, V> getVersioningColumn() {
			return versioningColumn;
		}
		
		public Serie<V> getVersioningSerie() {
			return versioningSerie;
		}
	}
}
