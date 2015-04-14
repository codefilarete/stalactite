package org.stalactite.lang.bean.safemodel;

import java.lang.reflect.Field;

import org.stalactite.lang.StringAppender;
import org.stalactite.lang.exception.Exceptions;
import org.stalactite.reflection.AccessorByField;
import org.stalactite.reflection.AccessorByMethod;
import org.stalactite.reflection.AccessorForList;
import org.stalactite.reflection.PropertyAccessor;

/**
 * @author Guillaume Mary
 */
public class MetaModel<O extends MetaModel> {
	
	protected static AccessorByField getDeclaredField(Class clazz, String name) {
		try {
			return new AccessorByField(clazz.getDeclaredField(name));
		} catch (NoSuchFieldException e) {
			Exceptions.throwAsRuntimeException(e);
			return null;
		}
	}
	
	public static String path(MetaModel metaModel) {
		StringAppender path = new StringAppender(100);
		MetaModel owner = metaModel;
		while(owner != null && owner.getAccessor() != null) {
			PropertyAccessor accessor = owner.getAccessor();
			String accessorDescription = accessor.getGetter().getName();
			if (accessor instanceof AccessorByMethod) {
				accessorDescription += "(";
				if (accessor instanceof AccessorForList) {
					accessorDescription += ((AccessorForList) accessor).getIndex();
				}
				accessorDescription += ")";
			}
			path.cat(0, ".", accessorDescription);
			owner = owner.getOwner();
		}
		path.cutHead(1);
		return path.toString();
	}
	
	private PropertyAccessor accessor;
	
	private O owner;
	
	public MetaModel() {
	}
	
	public MetaModel(PropertyAccessor accessor) {
		this.accessor = accessor;
	}
	
	protected void fixFieldsOwner() {
		for (Field field : this.getClass().getDeclaredFields()) {
			if (MetaModel.class.isAssignableFrom(field.getType())) {
				try {
					MetaModel o = (MetaModel) field.get(this);
					o.setOwner(this);
				} catch (IllegalAccessException e) {
					Exceptions.throwAsRuntimeException(e);
				}
			}
		}
	}
	
	public PropertyAccessor getAccessor() {
		return accessor;
	}
	
	public O getOwner() {
		return owner;
	}
	
	protected void setOwner(O owner) {
		this.owner = owner;
	}
}
