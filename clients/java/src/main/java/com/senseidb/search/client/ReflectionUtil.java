package com.senseidb.search.client;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ReflectionUtil {
    
    
    public static Set<Annotation> getAnnotations(Class cls) {
        Set<Annotation> ret = new HashSet<Annotation>();
        ret.addAll(Arrays.asList(cls.getAnnotations()));
        if(cls.getSuperclass() != null) {
            ret.addAll(getAnnotations(cls.getSuperclass()));            
        }
        for (Class intrface : cls.getInterfaces()) {
            ret.addAll(getAnnotations(intrface));     
        }
        return ret;
    }
    public static Annotation getAnnotation(Class cls, Class annotationCls) {
        if (cls == null) {
            return null;
        }
        Annotation ret = cls.getAnnotation(annotationCls);
        if (ret != null) {
            return ret;
        }
        ret = getAnnotation(cls.getSuperclass(), annotationCls);
        if (ret != null) {
            return ret;
        }        
        for (Class intrface : cls.getInterfaces()) {
            ret = getAnnotation(intrface, annotationCls);
            if (ret != null) {
                return ret;
            }  
        }
        return null;
    }
}
