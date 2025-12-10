package org.codefilarete.stalactite.engine.configurer.builder.embeddable;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration.Linkage;
import org.codefilarete.stalactite.dsl.key.CompositeKeyMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.IndexNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry.EnumBindType;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.ReadOnlyIterator;
import org.codefilarete.tool.function.Converter;

import static org.codefilarete.tool.Nullable.nullable;

public interface EmbeddableMappingConfiguration<C> {
	
	static <C> EmbeddableMappingConfiguration<C> fromCompositeKeyMappingConfiguration(CompositeKeyMappingConfiguration<C> compositeKeyMappingConfiguration) {
		return new EmbeddableMappingConfiguration<C>() {
			
			private final List<EmbeddableLinkage> linkages = Iterables.collectToList(compositeKeyMappingConfiguration.getPropertiesMapping(), embeddableLinkage -> new EmbeddableLinkage() {
				@Override
				public ReversibleAccessor getAccessor() {
					return embeddableLinkage.getAccessor();
				}
				
				@Nullable
				@Override
				public Field getField() {
					// this code serves little purpose since mapKey() doesn't give access to field override, maybe EmbeddableLinkage
					// contract should be specialized for Key mapping.
					return embeddableLinkage.getField();
				}
				
				@Nullable
				@Override
				public String getColumnName() {
					return embeddableLinkage.getColumnName();
				}
				
				@Nullable
				@Override
				public Size getColumnSize() {
					return embeddableLinkage.getColumnSize();
				}
				
				@Override
				public Class getColumnType() {
					return embeddableLinkage.getColumnType();
				}
				
				@Override
				public String getExtraTableName() {
					// we have to return null here since this method is used by EmbeddableMappingBuilder to keep
					// properties of main table
					// TODO : a better EmbeddableLinkage API
					return null;
				}
				
				@Nullable
				@Override
				public ParameterBinder<Object> getParameterBinder() {
					return embeddableLinkage.getParameterBinder();
				}
				
				@Nullable
				@Override
				public EnumBindType getEnumBindType() {
					return embeddableLinkage.getEnumBindType();
				}
				
				@Override
				public Boolean isNullable() {
					return false;
				}
				
				@Override
				public boolean isReadonly() {
					return false;
				}
				
				@Override
				public boolean isUnique() {
					return false;
				}
				
				@Nullable
				@Override
				public Converter getReadConverter() {
					// no special converter for composite key (makes no sense), not available by API too
					return null;
				}
				
				@Nullable
				@Override
				public Converter getWriteConverter() {
					// no special converter for composite key (makes no sense), not available by API too
					return null;
				}
			});
			
			@Override
			public Class<C> getBeanType() {
				return compositeKeyMappingConfiguration.getBeanType();
			}
			
			@Override
			@Nullable
			public EmbeddableMappingConfiguration<? super C> getMappedSuperClassConfiguration() {
				return nullable(compositeKeyMappingConfiguration.getMappedSuperClassConfiguration())
						.map(EmbeddableMappingConfiguration::fromCompositeKeyMappingConfiguration)
						.get();
			}
			
			@Override
			public List<EmbeddableLinkage> getPropertiesMapping() {
				return linkages;
			}
			
			@Override
			public Collection<Inset<C, Object>> getInsets() {
				return Iterables.collectToList(compositeKeyMappingConfiguration.getInsets(), compositeInset -> new Inset<C, Object>() {
					@Override
					public PropertyAccessor<C, Object> getAccessor() {
						return compositeInset.getAccessor();
					}
					
					@Override
					public Method getInsetAccessor() {
						return compositeInset.getInsetAccessor();
					}
					
					@Override
					public Class<Object> getEmbeddedClass() {
						return compositeInset.getEmbeddedClass();
					}
					
					@Override
					public ValueAccessPointSet<C> getExcludedProperties() {
						return compositeInset.getExcludedProperties();
					}
					
					@Override
					public ValueAccessPointMap<C, String> getOverriddenColumnNames() {
						return compositeInset.getOverriddenColumnNames();
					}
					
					@Override
					public ValueAccessPointMap<C, Size> getOverriddenColumnSizes() {
						return compositeInset.getOverriddenColumnSizes();
					}
					
					@Override
					public ValueAccessPointMap<C, Column> getOverriddenColumns() {
						return compositeInset.getOverriddenColumns();
					}
					
					@Override
					public EmbeddableMappingConfiguration<Object> getConfiguration() {
						return fromCompositeKeyMappingConfiguration(compositeInset.getConfigurationProvider().getConfiguration());
					}
				});
			}
			
			@Override
			public IndexNamingStrategy getIndexNamingStrategy() {
				// no IndexNamingStrategy for compositeKey, this would be nonsense.
				return null;
			}
		};
	}
	
	static <C> EmbeddableMappingConfiguration<C> fromEmbeddableMappingConfiguration(org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration<C> embeddableMappingConfiguration) {
		return new EmbeddableMappingConfiguration<C>() {
			
			private final List<EmbeddableLinkage> linkages = Iterables.collectToList(embeddableMappingConfiguration.getPropertiesMapping(), EmbeddableLinkageSupport::new);
			
			@Override
			public Class<C> getBeanType() {
				return embeddableMappingConfiguration.getBeanType();
			}
			
			@Override
			@Nullable
			public EmbeddableMappingConfiguration<? super C> getMappedSuperClassConfiguration() {
				return nullable(embeddableMappingConfiguration.getMappedSuperClassConfiguration())
						.map(EmbeddableMappingConfiguration::fromEmbeddableMappingConfiguration)
						.get();
			}
			
			@Override
			public List<EmbeddableLinkage> getPropertiesMapping() {
				return linkages;
			}
			
			@Override
			public Collection<Inset<C, Object>> getInsets() {
				return Iterables.collectToList(embeddableMappingConfiguration.getInsets(), embeddableInset -> new Inset<C, Object>() {
					@Override
					public Accessor<C, Object> getAccessor() {
						return embeddableInset.getAccessor();
					}
					
					@Override
					public Method getInsetAccessor() {
						return embeddableInset.getInsetAccessor();
					}
					
					@Override
					public Class<Object> getEmbeddedClass() {
						return embeddableInset.getEmbeddedClass();
					}
					
					@Override
					public ValueAccessPointSet<C> getExcludedProperties() {
						return embeddableInset.getExcludedProperties();
					}
					
					@Override
					public ValueAccessPointMap<C, String> getOverriddenColumnNames() {
						return embeddableInset.getOverriddenColumnNames();
					}
					
					@Override
					public ValueAccessPointMap<C, Size> getOverriddenColumnSizes() {
						return embeddableInset.getOverriddenColumnSizes();
					}
					
					@Override
					public ValueAccessPointMap<C, Column> getOverriddenColumns() {
						return embeddableInset.getOverriddenColumns();
					}
					
					@Override
					public EmbeddableMappingConfiguration<Object> getConfiguration() {
						return fromEmbeddableMappingConfiguration(embeddableInset.getConfigurationProvider().getConfiguration());
					}
				});
			}
			
			@Override
			public IndexNamingStrategy getIndexNamingStrategy() {
				return embeddableMappingConfiguration.getIndexNamingStrategy();
			}
		};
	}
	
	
	Class<C> getBeanType();
	
	@SuppressWarnings("squid:S1452" /* Can't remove wildcard here because it requires to create a local generic "super" type which is forbidden */)
	@Nullable
	EmbeddableMappingConfiguration<? super C> getMappedSuperClassConfiguration();
	
	List<EmbeddableLinkage> getPropertiesMapping();
	
	Collection<Inset<C, Object>> getInsets();
	
	IndexNamingStrategy getIndexNamingStrategy();
	
	/**
	 * @return an iterable for all inheritance configurations, including this
	 */
	default Iterable<EmbeddableMappingConfiguration> inheritanceIterable() {
		
		return () -> new ReadOnlyIterator<EmbeddableMappingConfiguration>() {
			
			private EmbeddableMappingConfiguration next = EmbeddableMappingConfiguration.this;
			
			@Override
			public boolean hasNext() {
				return next != null;
			}
			
			@Override
			public EmbeddableMappingConfiguration next() {
				if (!hasNext()) {
					// comply with next() method contract
					throw new NoSuchElementException();
				}
				EmbeddableMappingConfiguration result = this.next;
				this.next = this.next.getMappedSuperClassConfiguration();
				return result;
			}
		};
	}
	
	class EmbeddableLinkageSupport<C, O> implements EmbeddableLinkage<C, O> {
		
		private final Linkage<C, O> dslLinkage;
		
		public EmbeddableLinkageSupport(Linkage<C, O> dslLinkage) {
			this.dslLinkage = dslLinkage;
		}
		
		public Linkage<C, O> getDslLinkage() {
			return dslLinkage;
		}
		
		@Override
		public ReversibleAccessor<C, O> getAccessor() {
			return dslLinkage.getAccessor();
		}
		
		@Nullable
		@Override
		public Field getField() {
			return dslLinkage.getField();
		}
		
		@Nullable
		@Override
		public String getColumnName() {
			return dslLinkage.getColumnName();
		}
		
		@Nullable
		@Override
		public Size getColumnSize() {
			return dslLinkage.getColumnSize();
		}
		
		@Override
		public Class<O> getColumnType() {
			return dslLinkage.getColumnType();
		}
		
		@Nullable
		@Override
		public String getExtraTableName() {
			return dslLinkage.getExtraTableName();
		}
		
		@Override
		@Nullable
		public ParameterBinder<Object> getParameterBinder() {
			return dslLinkage.getParameterBinder();
		}
		
		@Nullable
		@Override
		public EnumBindType getEnumBindType() {
			return dslLinkage.getEnumBindType();
		}
		
		@Nullable
		@Override
		public Boolean isNullable() {
			return dslLinkage.isNullable();
		}
		
		@Override
		public boolean isReadonly() {
			return dslLinkage.isReadonly();
		}
		
		@Override
		public boolean isUnique() {
			return dslLinkage.isUnique();
		}
		
		@Nullable
		@Override
		public Converter<?, O> getReadConverter() {
			return dslLinkage.getReadConverter();
		}
		
		@Nullable
		@Override
		public Converter<O, ?> getWriteConverter() {
			return dslLinkage.getWriteConverter();
		}
	}
}
