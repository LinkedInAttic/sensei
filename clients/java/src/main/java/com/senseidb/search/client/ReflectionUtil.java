/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */

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
