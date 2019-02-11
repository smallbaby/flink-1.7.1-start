package com.jason.example;


import javafx.concurrent.Task;
import junit.framework.Test;
import scala.tools.nsc.Global;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@FunctionalInterface
// 函数式接口只能有一个抽象方法，而不是只能有一个方法
        // @FunctionalInterface 用于Service表明Service接口是一个函数式接口
interface Hello {
    public abstract void run();

    // 接口默认方法
    default void eat() {
        System.out.println("human eat default method");
    }
}

class Man implements Hello {
    @Override
    public void run() {
        System.out.println("override Run.....");
    }
}

class Student {
    String stuName;
    int age;
    String country;

    public String getStuName() {
        return stuName;
    }

    public void setStuName(String stuName) {
        this.stuName = stuName;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }
}

interface Hunman {
    void say(String str);
}

interface MathOperator<T> {
    T operator(T x, T y);
}

class User {
    String name;

    public User(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
}


interface UserFactory<T extends User> {
    T create(String username);
}


// CompletableFuture
class TaskRun implements Runnable {

    CompletableFuture<Integer> cf = null;

    public TaskRun(CompletableFuture<Integer> cf) {
        this.cf = cf;
    }

    @Override
    public void run() {
        int tmp = 0;
        try {
            tmp = cf.get() * cf.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


public class TestJdk8 {
    public static List<Student> init() {
        List<Student> students = new ArrayList<>();
        Student stu = new Student();
        stu.setStuName("stu1");
        stu.setAge(22);
        stu.setCountry("China");
        students.add(stu);
        stu = new Student();
        stu.setStuName("stu2");
        stu.setAge(25);
        stu.setCountry("USA");
        students.add(stu);
        stu = new Student();
        stu.setStuName("stu3");
        stu.setAge(23);
        stu.setCountry("China");
        students.add(stu);
        return students;
    }

    //判断一个数是否为质数，是返回true，不是返回false
    private static boolean isPrime(int number) {
        int x = number;
        if (x < 2) {
            return false;
        }
        for (int i = 2; i <= Math.sqrt(x); ++i) {
            if (x % i == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * 测试future
     */
    private static void testFuture() {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        new Thread(new TaskRun(future)).start();
        try {
            Thread.sleep(1000);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        future.complete(100);
    }

    private static void test1() {
        // A 冗长
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                Hello man = new Man();
                man.run();
                man.eat();
            }
        };
        new Thread(runnable).start();
        // lambda表达式
        new Thread(() -> System.out.println("helloworldxxxxx")).start();
        Hunman h = str -> System.out.println(str);
        h.say("hhhhhh");
        MathOperator<Integer> m = (x, y) -> {
            return x + y;
        };
        Integer xx = m.operator(1, 2);
        System.out.println(xx);
        // 方法引用
        List<String> strs = Arrays.asList("aa", "bb", "cc");
        strs.forEach(System.out::println);

        UserFactory<User> uf = User::new;
        List<User> users = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            users.add(uf.create("user" + i));
        }
        users.stream().map(User::getName).forEach(System.out::println);

        List<String> strss = Stream.of("a", "b", "c").collect(Collectors.toList());

        List<String> strsss = Stream.of("a", "b", "c").map(str -> str.toUpperCase()).collect(Collectors.toList());
        strsss.forEach(System.out::println);


        List<Student> ss = init();
        ss.stream().filter(x -> x.getAge() > 22).forEach(x -> {
            System.out.println(x.getStuName());
        });

        // reduce
        int ii = Stream.of(1, 2, 3, 4).filter(x -> x > 1).reduce(0, (num, x) -> num + x);
        System.out.println(ii);

        // 并行化流
        // 统计质数个数 串行
        long cnt = IntStream.range(1, 100000).filter(TestJdk8::isPrime).count();
        // 并行
        long cnt1 = IntStream.range(1, 10000).parallel().filter(TestJdk8::isPrime).count();
        long cnt2 = IntStream.range(1, 100).parallel().sum();
        // 并行排序
        // Arrays.parallelSort
    }


    public static void main(String[] args) {
        testFuture();

    }
    // lambda 表达式语法
    /**

     (Type1 param1 , Type2 param2, ....) -> {
     statment1;
     statment2;
     ....
     return statmentM;
     }
     1. 当木有参数时，直接使用 ()
     () -> {//......}
     2. 当只有一个参数时，可省略参数括号和类型
     param1 -> {
     ....
     return statmentM;
     }
     3. 只有一条语句时，{} 可省略
     param1 -> statment;
     4. 参数类型可省略，编译器可从上下文推断出类型
     (param1, param2) -> {
     //
     }

     方法引用
     简化lambda表达式的一种手段
     使用方法：
     :: 前边表示类名或实例名
     [].forEach(System.out::println)

     collect 由stream生成一个列表
     map 转换类型
     filter 过滤操作

     flatMap 每个元素转换得到stream对象，子stream中的元素压缩到父集中

     reduce 从一组值生成一个值
     Stream.of(1,2,3,4).reduce(0,(sum, x)->sum+x);

     */
}


