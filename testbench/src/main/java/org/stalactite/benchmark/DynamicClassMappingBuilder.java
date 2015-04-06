package org.stalactite.benchmark;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.stalactite.persistence.id.sequence.PooledSequenceIdentifierGenerator;
import org.stalactite.persistence.id.sequence.PooledSequenceIdentifierGeneratorOptions;
import org.stalactite.persistence.id.sequence.PooledSequencePersistenceOptions;
import org.stalactite.persistence.mapping.ClassMappingStrategy;
import org.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.ClassLoadingStrategy;
import net.bytebuddy.dynamic.DynamicType.Builder;

/**
 * @author Guillaume Mary
 */
public class DynamicClassMappingBuilder implements IMappingBuilder {
	
	private DynamicTable targetTable;
	Class<? extends DynamicEntity> dynamicType;
	List<Field> fields;
	
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

		fields = new ArrayList<>();
		Field[] declaredFields = dynamicType.getDeclaredFields();
		for (Field declaredField : declaredFields) {
			if (declaredField.getName().startsWith("q")) {
				declaredField.setAccessible(true);
				fields.add(declaredField);
			}
		}
		
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<Field, Column> fieldColumnMap = persistentFieldHarverster.mapFields(dynamicType, targetTable);
		ClassMappingStrategy<?> classMappingStrategy = new ClassMappingStrategy<>(dynamicType, targetTable, fieldColumnMap, new PooledSequenceIdentifierGenerator(new PooledSequenceIdentifierGeneratorOptions(100, "Toto", PooledSequencePersistenceOptions.DEFAULT)));
		return classMappingStrategy;
	}
	
	public static class DynamicTable extends Table {
		
		public final Column id;
		public final BidiMap<Long, Column> dynamicColumns = new DualHashBidiMap<>();
		
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
