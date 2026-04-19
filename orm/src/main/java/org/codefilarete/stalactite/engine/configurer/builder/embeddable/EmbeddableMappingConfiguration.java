package org.codefilarete.stalactite.engine.configurer.builder.embeddable;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration.Linkage;
import org.codefilarete.stalactite.dsl.key.CompositeKeyMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.UniqueConstraintNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
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
				public ReadWritePropertyAccessPoint getAccessor() {
					return embeddableLinkage.getAccessor();
				}
				
				@Nullable
				@Override
				public String getFieldName() {
					return embeddableLinkage.getFieldName();
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
				public Table getExtraTable() {
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
				
				@Override
				public boolean isSetByConstructor() {
					// for now, properties set by constructor on composite keys are not supported (no method on API for it)
					// TODO: implement set-by-cosntructor feature for composite key
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
			public <O> List<EmbeddableLinkage<C, O>> getPropertiesMapping() {
				return (List<EmbeddableLinkage<C,O>>) (List) linkages;
			}
			
			@Override
			public <O> Collection<Inset<C, O>> getInsets() {
				return Iterables.collectToList(compositeKeyMappingConfiguration.<O>getInsets(), compositeInset -> new Inset<C, O>() {
					@Override
					public ReadWritePropertyAccessPoint<C, O> getAccessor() {
						return compositeInset.getAccessor();
					}
					
					@Override
					public Method getInsetAccessor() {
						return compositeInset.getInsetAccessor();
					}
					
					@Override
					public Class<O> getEmbeddedClass() {
						return compositeInset.getEmbeddedClass();
					}
					
					@Override
					public ValueAccessPointSet<O, ValueAccessPoint<O>> getExcludedProperties() {
						return compositeInset.getExcludedProperties();
					}
					
					@Override
					public ValueAccessPointMap<O, String, ValueAccessPoint<O>> getOverriddenColumnNames() {
						return compositeInset.getOverriddenColumnNames();
					}
					
					@Override
					public ValueAccessPointMap<O, Size, ValueAccessPoint<O>> getOverriddenColumnSizes() {
						return compositeInset.getOverriddenColumnSizes();
					}
					
					@Override
					public ValueAccessPointMap<O, Column, ValueAccessPoint<O>> getOverriddenColumns() {
						return compositeInset.getOverriddenColumns();
					}
					
					@Override
					public EmbeddableMappingConfiguration<O> getConfiguration() {
						return fromCompositeKeyMappingConfiguration(compositeInset.getConfigurationProvider().getConfiguration());
					}
				});
			}
			
			@Override
			public UniqueConstraintNamingStrategy getUniqueConstraintNamingStrategy() {
				// no UniqueConstraintNamingStrategy for compositeKey, this would be nonsense.
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
			public <O> List<EmbeddableLinkage<C, O>> getPropertiesMapping() {
				return (List<EmbeddableLinkage<C, O>>) (List) linkages;
			}
			
			@Override
			public <O> Collection<Inset<C, O>> getInsets() {
				return Iterables.collectToList(embeddableMappingConfiguration.<O>getInsets(), embeddableInset -> new Inset<C, O>() {
					@Override
					public ReadWritePropertyAccessPoint<C, O> getAccessor() {
						return embeddableInset.getAccessor();
					}
					
					@Override
					public Method getInsetAccessor() {
						return embeddableInset.getInsetAccessor();
					}
					
					@Override
					public Class<O> getEmbeddedClass() {
						return embeddableInset.getEmbeddedClass();
					}
					
					@Override
					public ValueAccessPointSet<O, ValueAccessPoint<O>> getExcludedProperties() {
						return embeddableInset.getExcludedProperties();
					}
					
					@Override
					public ValueAccessPointMap<O, String, ValueAccessPoint<O>> getOverriddenColumnNames() {
						return embeddableInset.getOverriddenColumnNames();
					}
					
					@Override
					public ValueAccessPointMap<O, Size, ValueAccessPoint<O>> getOverriddenColumnSizes() {
						return embeddableInset.getOverriddenColumnSizes();
					}
					
					@Override
					public ValueAccessPointMap<O, Column, ValueAccessPoint<O>> getOverriddenColumns() {
						return embeddableInset.getOverriddenColumns();
					}
					
					@Override
					public EmbeddableMappingConfiguration<O> getConfiguration() {
						return fromEmbeddableMappingConfiguration(embeddableInset.getConfigurationProvider().getConfiguration());
					}
				});
			}
			
			@Override
			public UniqueConstraintNamingStrategy getUniqueConstraintNamingStrategy() {
				return embeddableMappingConfiguration.getUniqueConstraintNamingStrategy();
			}
		};
	}
	
	
	Class<C> getBeanType();
	
	@SuppressWarnings("squid:S1452" /* Can't remove wildcard here because it requires to create a local generic "super" type which is forbidden */)
	@Nullable
	EmbeddableMappingConfiguration<? super C> getMappedSuperClassConfiguration();
	
	<O> List<EmbeddableLinkage<C, O>> getPropertiesMapping();
	
	<O> Collection<Inset<C, O>> getInsets();
	
	UniqueConstraintNamingStrategy getUniqueConstraintNamingStrategy();
	
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
		public ReadWritePropertyAccessPoint<C, O> getAccessor() {
			return dslLinkage.getAccessor();
		}
		
		@Nullable
		@Override
		public String getFieldName() {
			return dslLinkage.getFieldName();
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
		public Table getExtraTable() {
			return dslLinkage.getExtraTable();
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
		
		@Override
		public boolean isSetByConstructor() {
			return dslLinkage.isSetByConstructor();
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
