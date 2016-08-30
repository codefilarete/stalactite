package org.gama.stalactite.benchmark;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.gama.lang.bean.IFactory;
import org.gama.lang.collection.ValueFactoryHashMap;
import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.benchmark.DynamicAndComplementaryClassMappingBuilder.DynamicEntity;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.NoopInsertListener;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.ddl.DDLGenerator;
import org.gama.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class BenchmarkDynamicComplementaryTables extends AbstractBenchmark<DynamicEntity> {
	
	private DynamicAndComplementaryClassMappingBuilder dynamicClassMappingBuilder;
	
	public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
		new BenchmarkDynamicComplementaryTables().run();
	}
	
	BenchmarkDynamicComplementaryTables() {
		super(10, 20000);
	}
	
	@Override
	protected void dropAndDeployDDL(final PersistenceContext persistenceContext) throws SQLException {
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.getDdlSchemaGenerator().addDDLGenerators(new DDLGenerator() {
			@Override
			public List<String> getCreationScripts() {
				return Collections.emptyList();
			}
			
			@Override
			public List<String> getDropScripts() {
				List<String> result = new ArrayList<>(DynamicEntity.QUESTION_COUNT);
				DDLTableGenerator ddlTableGenerator = new DDLTableGenerator(persistenceContext.getDialect().getJavaTypeToSqlTypeMapping());
				for (int i = 0; i < DynamicEntity.QUESTION_COUNT; i++) {
					result.add(ddlTableGenerator.generateDropTable(new Table(null, "DynamicTable_q" + i)));
				}
				return result;
			}
		});
		ddlDeployer.dropDDL();
		ddlDeployer.deployDDL();
	}
	
	@Override
	protected void appendMappingStrategy(PersistenceContext persistenceContext) {
		super.appendMappingStrategy(persistenceContext);
		persistenceContext.add(dynamicClassMappingBuilder.getClassMappingStrategy_nil());
		for (ClassMappingStrategy classMappingStrategy : dynamicClassMappingBuilder.indexDynamicTypes.values()) {
			persistenceContext.add(classMappingStrategy);
		}
		addListener(persistenceContext);
	}
	
	private <D extends DynamicEntity> void addListener(final PersistenceContext persistenceContext) {
		final Persister<D, Long> dynamicTypePersister = (Persister<D, Long>) persistenceContext.getPersister((Class<D>) dynamicClassMappingBuilder.dynamicType);
		dynamicTypePersister.getPersisterListener().addInsertListener(new NoopInsertListener<D>() {
			@Override
			public void afterInsert(Iterable<D> iterables) {
				Map<Field, List<DynamicEntity>> indexDynamicEntities = new ValueFactoryHashMap<>(10, new IFactory<Field, List<DynamicEntity>>() {
					@Override
					public List<DynamicEntity> createInstance(Field input) {
						return new ArrayList<>(500);
					}
				});
				for (D dynamicEntity : iterables) {
					try {
						Map<Field, DynamicEntity> localIndexDynamicEntity = indexFieldValue.get(dynamicEntity);
						for (Map.Entry<Field, DynamicEntity> dynamicEntityEntry : localIndexDynamicEntity.entrySet()) {
//								dynamicClassMappingBuilder.getClassMappingStrategy_indexes().get(dynamicEntityEntry.getKey())
							DynamicEntity indexDynamicEntity = dynamicEntityEntry.getValue();
							indexDynamicEntity.setId(dynamicEntity.getId());
							indexDynamicEntities.get(dynamicEntityEntry.getKey()).add(indexDynamicEntity);
							
						}
					} catch (Throwable t) {
						throw Exceptions.asRuntimeException(t);
					}
				}
				// NB: on force insert car ... on le sait
				// NB2: le système ne cherchera pas à générer un id car c'est configurer en AutoAssignedIdentifierGenerator
				for (Field field : indexDynamicEntities.keySet()) {
					Class<DynamicEntity> classToPersist = (Class<DynamicEntity>) dynamicClassMappingBuilder.indexDynamicTypes.get(field).getClassToPersist();
					persistenceContext.getPersister(classToPersist).insert(indexDynamicEntities.get(field));
				}
			}
		});
		
		dynamicTypePersister.getPersisterListener().addInsertListener(new NoopInsertListener<D>() {
			@Override
			public void afterInsert(Iterable<D> iterables) {
				List<DynamicEntity> nilDynamicEntities = new ArrayList<>(500);
				for (D dynamicEntity : iterables) {
					try {
						DynamicEntity nilDynamicEntity = dynamicClassMappingBuilder.nilDynamicType.newInstance();
						for (Field nilDynamicTypeField : dynamicClassMappingBuilder.nilDynamicTypeFields.values()) {
							Field dynamicTypeField = dynamicClassMappingBuilder.dynamicTypeFields.get(nilDynamicTypeField.getName());
							dynamicTypeField.setAccessible(true);
							Object value = dynamicTypeField.get(dynamicEntity);
							if (!dynamicTypeField.isAnnotationPresent(DynamicAndComplementaryClassMappingBuilder.Id.class)) {
								// question_id transformé en boolean
								value = ((int) value) < 0;
							}
							nilDynamicTypeField.setAccessible(true);
							nilDynamicTypeField.set(nilDynamicEntity, value);
						}
						nilDynamicEntities.add(nilDynamicEntity);
					} catch (Throwable t) {
						throw Exceptions.asRuntimeException(t);
					}
				}
				// NB: on force insert car ... on le sait
				// NB2: le système ne cherchera pas à générer un id car c'est configurer en AutoAssignedIdentifierGenerator
				persistenceContext.getPersister((Class<DynamicEntity>) dynamicClassMappingBuilder.nilDynamicType).insert(nilDynamicEntities);
				}
			});
	}
	
	@Override
	protected Callable<DynamicEntity> newCallableDataGenerator() {
		return new CallableDataGenerator();
	}
	
	@Override
	protected Callable<Void> newCallableDataPersister(List<DynamicEntity> data) {
		return new CallableDataInsert(data, PersistenceContexts.getCurrent());
	}
	
	@Override
	protected IMappingBuilder newMappingBuilder() {
		this.dynamicClassMappingBuilder = new DynamicAndComplementaryClassMappingBuilder();
		return this.dynamicClassMappingBuilder;
	}
	
	private static final Map<DynamicEntity, Map<Field, DynamicEntity>> indexFieldValue = new ValueFactoryHashMap<>(new IFactory<DynamicEntity, Map<Field, DynamicEntity>>() {
		@Override
		public Map<Field, DynamicEntity> createInstance(DynamicEntity input) {
			return new HashMap<>();
		}
	});
	
	public class CallableDataGenerator implements Callable<DynamicEntity> {
		
		private final DataGenerator dataGenerator = new DataGenerator(null);
		
		public CallableDataGenerator() {
		}
		
		@Override
		public DynamicEntity call() throws Exception {
			DynamicEntity dynamicEntity = dynamicClassMappingBuilder.dynamicType.newInstance();
			for (Field field : dynamicClassMappingBuilder.dynamicTypeFields.values()) {
				if (!field.isAnnotationPresent(DynamicAndComplementaryClassMappingBuilder.Id.class)) {
					Object value = dataGenerator.randomInteger(null);
					field.set(dynamicEntity, value);
					// alimentation de l'entité "index"
					ClassMappingStrategy classMappingStrategy = dynamicClassMappingBuilder.indexDynamicTypes.get(field);
					if (classMappingStrategy != null) {
						Class classToPersist = classMappingStrategy.getClassToPersist();
						Field field1 = classToPersist.getField(field.getName());
						Object indexInstance = classToPersist.newInstance();
						field1.set(indexInstance, value);
						synchronized (indexFieldValue) {
							indexFieldValue.get(dynamicEntity).put(field, (DynamicEntity) indexInstance);
						}
					}
				}
			}
			return dynamicEntity;
		}
	}
	
	public class CallableDataInsert<D extends DynamicEntity> extends CallableDataPersister<D> {
		
		public CallableDataInsert(List<D> data, PersistenceContext persistenceContext) {
			super(data, persistenceContext);
		}
		
		protected void persist(final PersistenceContext persistenceContext, List<D> data) {
			final Persister<D, Long> dynamicTypePersister = (Persister<D, Long>) persistenceContext.getPersister((Class<D>) dynamicClassMappingBuilder.dynamicType);
//			dynamicTypePersister.getPersisterListener().addInsertListener(new NoopInsertListener<D>() {
//				@Override
//				public void afterInsert(Iterable<D> iterables) {
//					Map<Field, List<DynamicEntity>> indexDynamicEntities = new ValueFactoryHashMap<>(10, new IFactory<Field, List<DynamicEntity>>() {
//						@Override
//						public List<DynamicEntity> createInstance(Field input) {
//							return new ArrayList<>(500);
//						}
//					});
//					for (D dynamicEntity : iterables) {
//						try {
//							Map<Field, DynamicEntity> localIndexDynamicEntity = indexFieldValue.get(dynamicEntity);
//							for (Entry<Field, DynamicEntity> dynamicEntityEntry : localIndexDynamicEntity.entrySet()) {
////								dynamicClassMappingBuilder.getClassMappingStrategy_indexes().get(dynamicEntityEntry.getKey())
//								DynamicEntity indexDynamicEntity = dynamicEntityEntry.getValue();
//								indexDynamicEntity.setId(dynamicEntity.getId());
//								indexDynamicEntities.get(dynamicEntityEntry.getKey()).add(indexDynamicEntity);
//								
//							}
//						} catch (Throwable t) {
//							throw Exceptions.asRuntimeException(t);
//						}
//					}
//					// NB: on force insert car ... on le sait
//					// NB2: le système ne cherchera pas à générer un id car c'est configurer en AutoAssignedIdentifierGenerator
//					for (Field field : indexDynamicEntities.keySet()) {
//						Class<DynamicEntity> classToPersist = (Class<DynamicEntity>) dynamicClassMappingBuilder.indexDynamicTypes.get(field).getClassToPersist();
//						persistenceContext.getPersister(classToPersist).insert(indexDynamicEntities.get(field));
//					}
//				}
//			});
//			dynamicTypePersister.setPersisterListener(new NoopPersisterListener<D>() {
//				@Override
//				public void afterInsert(Iterable<D> iterables) {
//					List<DynamicEntity> nilDynamicEntities = new ArrayList<>(500);
//					for (D dynamicEntity : iterables) {
//						try {
//							DynamicEntity nilDynamicEntity = dynamicClassMappingBuilder.nilDynamicType.newInstance();
//							for (Field nilDynamicTypeField : dynamicClassMappingBuilder.nilDynamicTypeFields.values()) {
//								Field dynamicTypeField = dynamicClassMappingBuilder.dynamicTypeFields.get(nilDynamicTypeField.getName());
//								Object value = dynamicTypeField.get(dynamicEntity);
//								if (!dynamicTypeField.isAnnotationPresent(DynamicAndComplementaryClassMappingBuilder.Id.class)) {
//									// question_id transformé en boolean
//									value = ((int) value) < 0;
//								}
//								nilDynamicTypeField.set(nilDynamicEntity, value);
//							}
//							nilDynamicEntities.add(nilDynamicEntity);
//						} catch (Throwable t) {
//							throw Exceptions.asRuntimeException(t);
//						}
//					}
//					// NB: on force insert car ... on le sait
//					// NB2: le système ne cherchera pas à générer un id car c'est configurer en AutoAssignedIdentifierGenerator
//					persistenceContext.getPersister((Class<DynamicEntity>) dynamicClassMappingBuilder.nilDynamicType).insert(nilDynamicEntities);
//				}
//			});
			dynamicTypePersister.persist((Iterable) data);
			
		}
	}
}
