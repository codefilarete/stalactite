package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.Date;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.OptimisticLockOption;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.model.Entity.Versioning;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.tool.function.Serie;

import static org.codefilarete.tool.Nullable.nullable;

public class VersioningStep<C, I, T extends Table<T>, V> {
	
	public Versioning<C, V, T> findVersioning(EntityMappingConfiguration<C, I> entityMappingConfiguration, T table, ColumnNamingStrategy columnNamingStrategy) {
		OptimisticLockOption<C, V> optimisticLockOption = (OptimisticLockOption<C, V>) entityMappingConfiguration.getOptimisticLockOption();
		Versioning<C, V, T> versioningStrategy = null;
		if (optimisticLockOption != null) {
			Serie<V> serie = nullable(optimisticLockOption.getSerie()).getOr(() -> findSerie(AccessorDefinition.giveDefinition(optimisticLockOption.getVersionAccessor()).getMemberType()));
			Column<T, V> versionColumn = buildColumn(optimisticLockOption.getVersionAccessor(), table, columnNamingStrategy);
			versioningStrategy = new Versioning<>(optimisticLockOption.getVersionAccessor(), versionColumn, serie);
		}
		
		return versioningStrategy;
	}
	
	private Column<T, V> buildColumn(ReadWritePropertyAccessPoint<C, V> versionAccessor, T table, ColumnNamingStrategy columnNamingStrategy) {
		AccessorDefinition versioningDefinition = AccessorDefinition.giveDefinition(versionAccessor);
		String versioningColumnName = columnNamingStrategy.giveName(versioningDefinition);
		boolean isColumnNullable = !Reflections.isPrimitiveType(versioningDefinition.getMemberType());
		// Column addition should be shared in EmbeddableMappingBuilder but the class is shared by different use cases for which the
		// versioning is not relevant, so this particularity is left here
		return table.addColumn(versioningColumnName, versioningDefinition.getMemberType(), null, isColumnNullable);
	}
	
	private Serie<V> findSerie(Class<V> propertyType) {
		Serie<V> serie;
		if (Integer.class.isAssignableFrom(propertyType) || int.class.isAssignableFrom(propertyType)) {
			serie = (Serie<V>) Serie.INTEGER_SERIE;
		} else if (Long.class.isAssignableFrom(propertyType) || long.class.isAssignableFrom(propertyType)) {
			serie = (Serie<V>) Serie.LONG_SERIE;
		} else if (Date.class.isAssignableFrom(propertyType)) {
			serie = (Serie<V>) Serie.NOW_SERIE;
		} else {
			throw new NotImplementedException("Type of versioned property is not implemented, please provide a "
					+ Serie.class.getSimpleName() + " for it : " + Reflections.toString(propertyType));
		}
		return serie;
	}
}
