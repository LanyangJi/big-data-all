package cn.jly.bigdata.utils;

import cn.hutool.core.io.FileUtil;
import cn.jly.bigdata.test.A;
import java.io.File;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author jilanyang
 * @date 2021/9/17 19:07
 */
public class TestMain {

  public static void main(String[] args) throws Exception {
    String packageName = A.class.getPackage().getName();
    Set<String> set = new HashSet<>();
    getFullClassNames(packageName, set);

    for (String s : set) {
      System.out.println(s);
    }

    System.out.println("------------");

    Set<Class> subClasses = getSubTypesOf(A.class);
    for (Class subClass : subClasses) {
      System.out.println("subClass = " + subClass);
    }

    System.out.println("------------");

    String replace = packageName.replace(".", "/");
    URL resource = TestMain.class.getClassLoader().getResource(replace);
    System.out.println(resource.getPath());
  }

  /**
   * 获取指定类当前包及其子包下的所有子类（包括当前类本身）
   *
   * @param clazz 指定类型
   * @return 当前类所在包及其子包下所有子类的Class类型（包括当前类本身）
   */
  public static Set<Class> getSubTypesOf(Class clazz) throws Exception {
    Set<Class> result = new HashSet<>();
    result.add(clazz);

    String packageName = clazz.getPackage().getName();
    // 获取当前包及其子包下的所有类的全类名
    Set<String> fullClassNameSet = new HashSet<>();
    getFullClassNames(packageName, fullClassNameSet);

    for (String fullClassName : fullClassNameSet) {
      Class<?> currentClazz = Class.forName(fullClassName);
      // 判断类型是否相同或者前者是否是后者的超类
      if (clazz.isAssignableFrom(currentClazz)) {
        result.add(currentClazz);
      }
    }

    return result;
  }

  /**
   * 获取指定包及其所有子包下的所有java类的全类名的集合
   *
   * @param packageName      指定包名
   * @param fullClassNameSet 全类名结果集
   */
  private static void getFullClassNames(final String packageName, final Set<String> fullClassNameSet) {
    // 相对路径（相对类路径）
    String relativePath = packageName.replace(".", "/");
    // 绝对路径
    String absolutePath = FileUtil.getAbsolutePath(relativePath);
    // 转file对象
    File file = FileUtil.newFile(absolutePath);
    // 存当前路径下的所有java文件全类名
    fullClassNameSet.addAll(transformFullClassName(packageName, FileUtil.listFileNames(absolutePath)));
    // 遍历子文件
    File[] subFiles = file.listFiles();
    if (subFiles != null && subFiles.length > 0) {
      // 遍历子file
      for (File subFile : subFiles) {
        // 如果是目录
        if (subFile.isDirectory()) {
          // 收集当前路径下的所有java类的全类名
          String currentPackage = packageName + "." + subFile.getName();
          fullClassNameSet.addAll(transformFullClassName(currentPackage, FileUtil.listFileNames(subFile.getAbsolutePath())));

          // 递归遍历子目录
          getFullClassNames(currentPackage, fullClassNameSet);
        }
      }
    }
  }

  /**
   * 包名和java类名的拼接，形成全类名
   *
   * @param packageName 包名
   * @param fileNames   该包下的所有文件名，这里包含后缀.java
   * @return 当前包下的所有全类名
   */
  public static Set<String> transformFullClassName(final String packageName, final List<String> fileNames) {
    return fileNames.stream().map(fn -> packageName + "." + FileUtil.getPrefix(fn))
        .collect(Collectors.toSet());
  }
}
