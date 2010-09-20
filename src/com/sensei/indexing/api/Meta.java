package com.sensei.indexing.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
public @interface Meta {
	String name() default "";
	MetaType type() default MetaType.String;
	String format() default "";
}
