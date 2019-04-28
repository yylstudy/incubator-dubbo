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
package com.alibaba.dubbo.common.compiler.support;

import com.alibaba.dubbo.common.utils.ClassHelper;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import javassist.LoaderClassPath;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JavassistCompiler. (SPI, Singleton, ThreadSafe)
 */
public class JavassistCompiler extends AbstractCompiler {

    private static final Pattern IMPORT_PATTERN = Pattern.compile("import\\s+([\\w\\.\\*]+);\n");

    private static final Pattern EXTENDS_PATTERN = Pattern.compile("\\s+extends\\s+([\\w\\.]+)[^\\{]*\\{\n");

    private static final Pattern IMPLEMENTS_PATTERN = Pattern.compile("\\s+implements\\s+([\\w\\.]+)\\s*\\{\n");

    private static final Pattern METHODS_PATTERN = Pattern.compile("\n(private|public|protected)\\s+");

    private static final Pattern FIELD_PATTERN = Pattern.compile("[^\n]+=[^\n]+;");

    /**
     * javassist方式动态编译生成的代码
     * @param name 类名（包含包名）
     * @param source 源代码字符串
     * @return
     * @throws Throwable
     */
    @Override
    public Class<?> doCompile(String name, String source) throws Throwable {
        int i = name.lastIndexOf('.');
        //类名
        String className = i < 0 ? name : name.substring(i + 1);
        ClassPool pool = new ClassPool(true);
        pool.appendClassPath(new LoaderClassPath(ClassHelper.getCallerClassLoader(getClass())));
        Matcher matcher = IMPORT_PATTERN.matcher(source);
        List<String> importPackages = new ArrayList<String>();
        Map<String, String> fullNames = new HashMap<String, String>();
        while (matcher.find()) {
            //导入包的包名
            String pkg = matcher.group(1);
            //import是以.*结尾的
            if (pkg.endsWith(".*")) {
                //获取.*前面的包名
                String pkgName = pkg.substring(0, pkg.length() - 2);
                //导入包名
                pool.importPackage(pkgName);
                importPackages.add(pkgName);
            } else {
                int pi = pkg.lastIndexOf('.');
                if (pi > 0) {
                    String pkgName = pkg.substring(0, pi);
                    //导入包名
                    pool.importPackage(pkgName);
                    importPackages.add(pkgName);
                    //添加类名和包名.类名的映射
                    fullNames.put(pkg.substring(pi + 1), pkg);
                }
            }
        }
        String[] packages = importPackages.toArray(new String[0]);
        matcher = EXTENDS_PATTERN.matcher(source);
        CtClass cls;
        //是否继承类名
        if (matcher.find()) {
            String extend = matcher.group(1).trim();
            String extendClass;
            if (extend.contains(".")) {
                extendClass = extend;
            } else if (fullNames.containsKey(extend)) {
                extendClass = fullNames.get(extend);
            } else {
                extendClass = ClassUtils.forName(packages, extend).getName();
            }
            cls = pool.makeClass(name, pool.get(extendClass));
        } else {
            //javassist创建类 name格式为 包名.类名
            cls = pool.makeClass(name);
        }
        //实现的接口
        matcher = IMPLEMENTS_PATTERN.matcher(source);
        if (matcher.find()) {
            //实现的接口数组
            String[] ifaces = matcher.group(1).trim().split("\\,");
            for (String iface : ifaces) {
                iface = iface.trim();
                String ifaceClass;
                //存在. 说明已经引入包名了
                if (iface.contains(".")) {
                    ifaceClass = iface;
                } else if (fullNames.containsKey(iface)) {
                    ifaceClass = fullNames.get(iface);
                } else {
                    ifaceClass = ClassUtils.forName(packages, iface).getName();
                }
                //从ClassPool中获取接口的CtClass
                cls.addInterface(pool.get(ifaceClass));
            }
        }
        //获取class中的代码，包括属性和方法
        String body = source.substring(source.indexOf("{") + 1, source.length() - 1);
        //获取方法体
        String[] methods = METHODS_PATTERN.split(body);
        for (String method : methods) {
            method = method.trim();
            if (method.length() > 0) {
                //类名开头，说明是构造方法
                if (method.startsWith(className)) {
                    cls.addConstructor(CtNewConstructor.make("public " + method, cls));
                }
                //属性，则添加属性
                else if (FIELD_PATTERN.matcher(method).matches()) {
                    cls.addField(CtField.make("private " + method, cls));
                }
                //添加方法
                else {
                    cls.addMethod(CtNewMethod.make("public " + method, cls));
                }
            }
        }
        return cls.toClass(ClassHelper.getCallerClassLoader(getClass()), JavassistCompiler.class.getProtectionDomain());
    }

}
