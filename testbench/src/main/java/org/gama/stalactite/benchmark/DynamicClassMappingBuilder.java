package org.gama.stalactite.benchmark;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.ClassLoadingStrategy;
import net.bytebuddy.dynamic.DynamicType.Builder;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.gama.stalactite.persistence.id.manager.BeforeInsertIdentifierManager;
import org.gama.stalactite.persistence.id.sequence.PooledHiLoSequence;
import org.gama.stalactite.persistence.id.sequence.PooledHiLoSequenceOptions;
import org.gama.stalactite.persistence.id.sequence.SequenceStorageOptions;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class DynamicClassMappingBuilder implements IMappingBuilder {
	
	private DynamicTable targetTable;
	Class<? extends DynamicEntity> dynamicType;
	Map<String, Field> fields;
	
	@Override
	public ClassMappingStrategy getClassMappingStrategy() {
		targetTable = new DynamicTable("DynamicTable");
		
		
		Builder<DynamicEntity> dynamicClassBuilder = new ByteBuddy().subclass(DynamicEntity.class);
		
		for (int i = 0; i < DynamicEntity.QUESTION_COUNT; i++) {
			dynamicClassBuilder = dynamicClassBuilder.defineField("q" + i, Integer.class, Modifier.PUBLIC);
		}
		
		dynamicType = dynamicClassBuilder
				//.name(getClass().getPackage().getName()+"Tata")
				.make()
				.load(getClass().getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
				.getLoaded();

		fields = new HashMap<>();
		Field[] declaredFields = dynamicType.getDeclaredFields();
		for (Field declaredField : declaredFields) {
			if (declaredField.getName().startsWith("q")) {
				declaredField.setAccessible(true);
				fields.put(declaredField.getName(), declaredField);
			}
		}
		
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<PropertyAccessor, Column> fieldColumnMap = persistentFieldHarverster.mapFields(dynamicType, targetTable);
		PersistenceContext currentPersistenceContext = PersistenceContexts.getCurrent();
		PropertyAccessor idAccessor = PropertyAccessor.forProperty(persistentFieldHarverster.getField("id"));
		PooledHiLoSequence longSequence = new PooledHiLoSequence(new PooledHiLoSequenceOptions(100, "Toto", 
				SequenceStorageOptions.DEFAULT),
				currentPersistenceContext.getDialect(), (SeparateTransactionExecutor) currentPersistenceContext.getConnectionProvider(),
				currentPersistenceContext.getJDBCBatchSize());
		ClassMappingStrategy<? extends DynamicEntity, Long> classMappingStrategy = new ClassMappingStrategy<>(dynamicType, targetTable,
				fieldColumnMap, idAccessor, new BeforeInsertIdentifierManager<>(IdMappingStrategy.toIdAccessor(idAccessor), longSequence, long.class));
		return classMappingStrategy;
	}
	
	public static class DynamicTable extends Table {
		
		public final Column id;
		public final Map<Long, Column> dynamicColumns = new HashMap<>();
		
		public DynamicTable(String tableName) {
			super(null, tableName);
			id = new Column("id", Long.TYPE);
			id.setPrimaryKey(true);
			for (int i = 0; i < 1; i++) {
				Class columnType = Integer.class;
				Column column = new Column("q" + i, columnType);
				dynamicColumns.put((long) i, column);
				if (i % 5 == 0) {
					new Index(column, "idx_" + column.getName());
				}
			}
		}
	}
	
	public static class DynamicEntity {
		
		public static int QUESTION_COUNT = 100;
		
		private Long id;
		
		public DynamicEntity() {
		}
		
		public void setId(long id) {
			this.id = id;
		}
	}
	
}
