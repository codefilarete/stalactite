package org.codefilarete.stalactite.engine.configurer.map;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.AccessorDefinitionDefiner;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.tool.function.TriConsumer;

/**
 * {@link Accessor} that converts Map&lt;K, V&gt; to Map&lt;KID, V&gt; on {@link #get(Object)}.
 * Could have been an anonymous class but {@link MapRelationConfigurer} requires to call {@link AccessorDefinition#giveDefinition(ValueAccessPoint)}
 * at some point, which causes {@link UnsupportedOperationException} since the anonymous class is unknown from it.
 * Though it as to be a named class, moreover {@link AccessorDefinition} has been enhanced take into account classes that provides their
 * {@link AccessorDefinition} by their own through {@link AccessorDefinitionDefiner}.
 *
 * @param <SRC> entity type owning the relation
 * @param <K1> Map key entity type
 * @param <V1> Map value type
 * @param <K2> converted Map key entity type
 * @param <V2> converted Map value type
 * @param <M> relation Map type
 * @param <MM> redefined Map type to get entity key identifier
 * @author Guillaume Mary
 */
class ConvertingMapAccessor<SRC, K1, V1, K2, V2, M extends Map<K1, V1>, MM extends Map<K2, V2>> implements Accessor<SRC, MM>, AccessorDefinitionDefiner<SRC> {
	
	private final MapRelation<SRC, K1, V1, M> map;
	
	private final AccessorDefinition accessorDefinition;
	
	private final TriConsumer<K1, V1, MM> converter;
	
	public ConvertingMapAccessor(MapRelation<SRC, K1, V1, M> map,
								 TriConsumer<K1, V1, MM> converter) {
		this.map = map;
		this.accessorDefinition = AccessorDefinition.giveDefinition(this.map.getMapProvider());
		this.converter = converter;
	}
	
	@Override
	public MM get(SRC SRC) {
		M m = map.getMapProvider().get(SRC);
		if (m != null) {
			// we can use an HashMap because K2 is expected to be an identifier :
			// either one of the entity, or an embeddable bean being a map key, so in both cases having a dedicated implementation of equals() + hashCode()
			MM result = (MM) new HashMap<>();
			m.forEach((k, v) -> converter.accept(k, v, result));
			return result;
		} else {
			return null;
		}
	}
	
	@Override
	public AccessorDefinition asAccessorDefinition() {
		return this.accessorDefinition;
	}
}