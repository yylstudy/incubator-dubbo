/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.common.bytecode;

import com.alibaba.dubbo.common.utils.ClassHelper;
import com.alibaba.dubbo.common.utils.ReflectUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;

/**
 * Wrapper.
 */
public abstract class Wrapper {
    private static final Map<Class<?>, Wrapper> WRAPPER_MAP = new ConcurrentHashMap<Class<?>, Wrapper>(); //class wrapper map
    private static final String[] EMPTY_STRING_ARRAY = new String[0];
    private static final String[] OBJECT_METHODS = new String[]{"getClass", "hashCode", "toString", "equals"};
    private static final Wrapper OBJECT_WRAPPER = new Wrapper() {
        @Override
        public String[] getMethodNames() {
            return OBJECT_METHODS;
        }

        @Override
        public String[] getDeclaredMethodNames() {
            return OBJECT_METHODS;
        }

        @Override
        public String[] getPropertyNames() {
            return EMPTY_STRING_ARRAY;
        }

        @Override
        public Class<?> getPropertyType(String pn) {
            return null;
        }

        @Override
        public Object getPropertyValue(Object instance, String pn) throws NoSuchPropertyException {
            throw new NoSuchPropertyException("Property [" + pn + "] not found.");
        }

        @Override
        public void setPropertyValue(Object instance, String pn, Object pv) throws NoSuchPropertyException {
            throw new NoSuchPropertyException("Property [" + pn + "] not found.");
        }

        @Override
        public boolean hasProperty(String name) {
            return false;
        }

        @Override
        public Object invokeMethod(Object instance, String mn, Class<?>[] types, Object[] args) throws NoSuchMethodException {
            if ("getClass".equals(mn)) return instance.getClass();
            if ("hashCode".equals(mn)) return instance.hashCode();
            if ("toString".equals(mn)) return instance.toString();
            if ("equals".equals(mn)) {
                if (args.length == 1) return instance.equals(args[0]);
                throw new IllegalArgumentException("Invoke method [" + mn + "] argument number error.");
            }
            throw new NoSuchMethodException("Method [" + mn + "] not found.");
        }
    };
    private static AtomicLong WRAPPER_CLASS_COUNTER = new AtomicLong(0);

    /**
     * get wrapper.
     *
     * @param c Class instance.
     * @return Wrapper instance(not null).
     */
    /**
     * 获取装饰类
     * @param c
     * @return
     */
    public static Wrapper getWrapper(Class<?> c) {
        while (ClassGenerator.isDynamicClass(c)) // can not wrapper on dynamic class.
            c = c.getSuperclass();

        if (c == Object.class)
            return OBJECT_WRAPPER;

        Wrapper ret = WRAPPER_MAP.get(c);
        if (ret == null) {
            //创建装饰类
            ret = makeWrapper(c);
            WRAPPER_MAP.put(c, ret);
        }
        return ret;
    }

    /**
     * 创建装饰类
     * @param c
     * @return
     */
    private static Wrapper makeWrapper(Class<?> c) {
        //是否是原始类（boolean、char、byte、short、int、long、float、double）
        if (c.isPrimitive())
            throw new IllegalArgumentException("Can not create wrapper for primitive type: " + c);

        String name = c.getName();
        ClassLoader cl = ClassHelper.getClassLoader(c);
        //setPropertyValue的方法代码
        StringBuilder c1 = new StringBuilder("public void setPropertyValue(Object o, String n, Object v){ ");
        //getPropertyValue的方法代码
        StringBuilder c2 = new StringBuilder("public Object getPropertyValue(Object o, String n){ ");
        //invokeMethod的方法代码
        StringBuilder c3 = new StringBuilder("public Object invokeMethod(Object o, String n, Class[] p, Object[] v) throws " + InvocationTargetException.class.getName() + "{ ");
        //生成类型转换及异常捕捉代码
        c1.append(name).append(" w; try{ w = ((").append(name).append(")$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }");
        c2.append(name).append(" w; try{ w = ((").append(name).append(")$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }");
        c3.append(name).append(" w; try{ w = ((").append(name).append(")$1); }catch(Throwable e){ throw new IllegalArgumentException(e); }");
        //属性名和属性类型的映射
        Map<String, Class<?>> pts = new HashMap<String, Class<?>>();
        //方法签名和方法对象的映射
        Map<String, Method> ms = new LinkedHashMap<String, Method>();
        //当前类和其父类、接口的所有public的方法名
        List<String> mns = new ArrayList<String>();
        //当前类的所有public的方法名
        List<String> dmns = new ArrayList<String>();

        // get all public field.
        for (Field f : c.getFields()) {
            String fn = f.getName();
            Class<?> ft = f.getType();
            if (Modifier.isStatic(f.getModifiers()) || Modifier.isTransient(f.getModifiers()))
                continue;
            //生成条件判断赋值语句 如 if($2.equals("name")){w.name=(java.lang.String) $3;return ;}
            c1.append(" if( $2.equals(\"").append(fn).append("\") ){ w.").append(fn).append("=").append(arg(ft, "$3")).append("; return; }");
            c2.append(" if( $2.equals(\"").append(fn).append("\") ){ return ($w)w.").append(fn).append("; }");
            pts.put(fn, ft);
        }

        Method[] methods = c.getMethods();
        // get all public method.
        //获取所有public方法
        boolean hasMethod = hasMethods(methods);
        if (hasMethod) {
            c3.append(" try{");
        }
        for (Method m : methods) {
            //忽略Object的方法
            if (m.getDeclaringClass() == Object.class)
                continue;

            String mn = m.getName();
            c3.append(" if( \"").append(mn).append("\".equals( $2 ) ");
            int len = m.getParameterTypes().length;
            c3.append(" && ").append(" $3.length == ").append(len);

            boolean override = false;
            //检测是否有方法重载
            for (Method m2 : methods) {
                if (m != m2 && m.getName().equals(m2.getName())) {
                    override = true;
                    break;
                }
            }
            if (override) {
                if (len > 0) {
                    for (int l = 0; l < len; l++) {
                        c3.append(" && ").append(" $3[").append(l).append("].getName().equals(\"")
                                .append(m.getParameterTypes()[l].getName()).append("\")");
                    }
                }
            }

            c3.append(" ) { ");

            if (m.getReturnType() == Void.TYPE)
                c3.append(" w.").append(mn).append('(').append(args(m.getParameterTypes(), "$4")).append(");").append(" return null;");
            else
                c3.append(" return ($w)w.").append(mn).append('(').append(args(m.getParameterTypes(), "$4")).append(");");

            c3.append(" }");

            mns.add(mn);
            if (m.getDeclaringClass() == c)
                dmns.add(mn);
            ms.put(ReflectUtils.getDesc(m), m);
        }
        if (hasMethod) {
            c3.append(" } catch(Throwable e) { ");
            c3.append("     throw new java.lang.reflect.InvocationTargetException(e); ");
            c3.append(" }");
        }

        c3.append(" throw new " + NoSuchMethodException.class.getName() + "(\"Not found method \\\"\"+$2+\"\\\" in class " + c.getName() + ".\"); }");

        // deal with get/set method.
        Matcher matcher;
        for (Map.Entry<String, Method> entry : ms.entrySet()) {
            String md = entry.getKey();
            Method method = (Method) entry.getValue();
            if ((matcher = ReflectUtils.GETTER_METHOD_DESC_PATTERN.matcher(md)).matches()) {
                String pn = propertyName(matcher.group(1));
                c2.append(" if( $2.equals(\"").append(pn).append("\") ){ return ($w)w.").append(method.getName()).append("(); }");
                pts.put(pn, method.getReturnType());
            } else if ((matcher = ReflectUtils.IS_HAS_CAN_METHOD_DESC_PATTERN.matcher(md)).matches()) {
                String pn = propertyName(matcher.group(1));
                c2.append(" if( $2.equals(\"").append(pn).append("\") ){ return ($w)w.").append(method.getName()).append("(); }");
                pts.put(pn, method.getReturnType());
            } else if ((matcher = ReflectUtils.SETTER_METHOD_DESC_PATTERN.matcher(md)).matches()) {
                Class<?> pt = method.getParameterTypes()[0];
                String pn = propertyName(matcher.group(1));
                c1.append(" if( $2.equals(\"").append(pn).append("\") ){ w.").append(method.getName()).append("(").append(arg(pt, "$3")).append("); return; }");
                pts.put(pn, pt);
            }
        }
        c1.append(" throw new " + NoSuchPropertyException.class.getName() + "(\"Not found property \\\"\"+$2+\"\\\" filed or setter method in class " + c.getName() + ".\"); }");
        c2.append(" throw new " + NoSuchPropertyException.class.getName() + "(\"Not found property \\\"\"+$2+\"\\\" filed or setter method in class " + c.getName() + ".\"); }");

        // make class
        //第一为1
        long id = WRAPPER_CLASS_COUNTER.getAndIncrement();
        //创建一个类生成器
        ClassGenerator cc = ClassGenerator.newInstance(cl);
        //设置class，一般是 com.alibaba.dubbo.common.bytecode.Wrapper1
        cc.setClassName((Modifier.isPublic(c.getModifiers()) ? Wrapper.class.getName() : c.getName() + "$sw") + id);
        cc.setSuperClass(Wrapper.class);

        cc.addDefaultConstructor();
        //添加属性
        cc.addField("public static String[] pns;"); // property name array.
        cc.addField("public static " + Map.class.getName() + " pts;"); // property type map.
        cc.addField("public static String[] mns;"); // all method name array.
        cc.addField("public static String[] dmns;"); // declared method name array.
        for (int i = 0, len = ms.size(); i < len; i++)
            cc.addField("public static Class[] mts" + i + ";");

        cc.addMethod("public String[] getPropertyNames(){ return pns; }");
        cc.addMethod("public boolean hasProperty(String n){ return pts.containsKey($1); }");
        cc.addMethod("public Class getPropertyType(String n){ return (Class)pts.get($1); }");
        cc.addMethod("public String[] getMethodNames(){ return mns; }");
        cc.addMethod("public String[] getDeclaredMethodNames(){ return dmns; }");
        cc.addMethod(c1.toString());
        cc.addMethod(c2.toString());
        cc.addMethod(c3.toString());

        try {
            //编译成class
            Class<?> wc = cc.toClass();
            // setup static field.
            //对生成的class 的静态属性 赋值
            wc.getField("pts").set(null, pts);
            wc.getField("pns").set(null, pts.keySet().toArray(new String[0]));
            wc.getField("mns").set(null, mns.toArray(new String[0]));
            wc.getField("dmns").set(null, dmns.toArray(new String[0]));
            int ix = 0;
            for (Method m : ms.values())
                wc.getField("mts" + ix++).set(null, m.getParameterTypes());
            return (Wrapper) wc.newInstance();
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            cc.release();
            ms.clear();
            mns.clear();
            dmns.clear();
        }
    }

    private static String arg(Class<?> cl, String name) {
        if (cl.isPrimitive()) {
            if (cl == Boolean.TYPE)
                return "((Boolean)" + name + ").booleanValue()";
            if (cl == Byte.TYPE)
                return "((Byte)" + name + ").byteValue()";
            if (cl == Character.TYPE)
                return "((Character)" + name + ").charValue()";
            if (cl == Double.TYPE)
                return "((Number)" + name + ").doubleValue()";
            if (cl == Float.TYPE)
                return "((Number)" + name + ").floatValue()";
            if (cl == Integer.TYPE)
                return "((Number)" + name + ").intValue()";
            if (cl == Long.TYPE)
                return "((Number)" + name + ").longValue()";
            if (cl == Short.TYPE)
                return "((Number)" + name + ").shortValue()";
            throw new RuntimeException("Unknown primitive type: " + cl.getName());
        }
        return "(" + ReflectUtils.getName(cl) + ")" + name;
    }

    private static String args(Class<?>[] cs, String name) {
        int len = cs.length;
        if (len == 0) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            if (i > 0)
                sb.append(',');
            sb.append(arg(cs[i], name + "[" + i + "]"));
        }
        return sb.toString();
    }

    private static String propertyName(String pn) {
        return pn.length() == 1 || Character.isLowerCase(pn.charAt(1)) ? Character.toLowerCase(pn.charAt(0)) + pn.substring(1) : pn;
    }

    private static boolean hasMethods(Method[] methods) {
        if (methods == null || methods.length == 0) {
            return false;
        }
        for (Method m : methods) {
            if (m.getDeclaringClass() != Object.class) {
                return true;
            }
        }
        return false;
    }

    /**
     * get property name array.
     *
     * @return property name array.
     */
    abstract public String[] getPropertyNames();

    /**
     * get property type.
     *
     * @param pn property name.
     * @return Property type or nul.
     */
    abstract public Class<?> getPropertyType(String pn);

    /**
     * has property.
     *
     * @param name property name.
     * @return has or has not.
     */
    abstract public boolean hasProperty(String name);

    /**
     * get property value.
     *
     * @param instance instance.
     * @param pn       property name.
     * @return value.
     */
    abstract public Object getPropertyValue(Object instance, String pn) throws NoSuchPropertyException, IllegalArgumentException;

    /**
     * set property value.
     *
     * @param instance instance.
     * @param pn       property name.
     * @param pv       property value.
     */
    abstract public void setPropertyValue(Object instance, String pn, Object pv) throws NoSuchPropertyException, IllegalArgumentException;

    /**
     * get property value.
     *
     * @param instance instance.
     * @param pns      property name array.
     * @return value array.
     */
    public Object[] getPropertyValues(Object instance, String[] pns) throws NoSuchPropertyException, IllegalArgumentException {
        Object[] ret = new Object[pns.length];
        for (int i = 0; i < ret.length; i++)
            ret[i] = getPropertyValue(instance, pns[i]);
        return ret;
    }

    /**
     * set property value.
     *
     * @param instance instance.
     * @param pns      property name array.
     * @param pvs      property value array.
     */
    public void setPropertyValues(Object instance, String[] pns, Object[] pvs) throws NoSuchPropertyException, IllegalArgumentException {
        if (pns.length != pvs.length)
            throw new IllegalArgumentException("pns.length != pvs.length");

        for (int i = 0; i < pns.length; i++)
            setPropertyValue(instance, pns[i], pvs[i]);
    }

    /**
     * get method name array.
     *
     * @return method name array.
     */
    abstract public String[] getMethodNames();

    /**
     * get method name array.
     *
     * @return method name array.
     */
    abstract public String[] getDeclaredMethodNames();

    /**
     * has method.
     *
     * @param name method name.
     * @return has or has not.
     */
    public boolean hasMethod(String name) {
        for (String mn : getMethodNames())
            if (mn.equals(name)) return true;
        return false;
    }

    /**
     * invoke method.
     *
     * @param instance instance.
     * @param mn       method name.
     * @param types
     * @param args     argument array.
     * @return return value.
     */
    abstract public Object invokeMethod(Object instance, String mn, Class<?>[] types, Object[] args) throws NoSuchMethodException, InvocationTargetException;
}