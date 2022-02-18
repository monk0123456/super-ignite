package org.tools;

import clojure.lang.RT;
import cn.mysuper.service.IMyPlusFunc;
import org.apache.ignite.Ignition;
import org.gridgain.myservice.MyPlusFuncImpl;

import java.io.Serializable;

public class MyPlusFunc implements Serializable {

    private static final long serialVersionUID = 175563786053447344L;
    private static IMyPlusFunc myPlusFunc = new MyPlusFuncImpl();

    /**
     * 获取 table 的自增长
     * */
    public static Long auto_id(final String tableName) {
        return Ignition.ignite().atomicSequence(tableName, 0, true).incrementAndGet();
    }

    /**
     * 获取序列的第 n 个
     * */
    public static Object nth(final Object coll, final int n) {
        return RT.nth(coll, n);
    }

    /**
     * 获取序列的第一个
     * */
    public static Object first(final Object lst) {
        return RT.first(lst);
    }

    /**
     * 显示信息
     * */
    public static String showMsg(final String msg)
    {
        return msg;
    }

    /**
     * 获取定时任务详细信息
     * */
    public static String getScheduler(final String schedulerName)
    {
        return myPlusFunc.getScheduler(schedulerName);
    }

    /**
     * 自定义函数的要求
     * 1、必须有一个默认构造函数
     * 2、方法名不能重载
     * */
    public static Object myFun(final String methodName, final Object... ps) {
        return myPlusFunc.myFun(methodName, ps);
    }

    /**
     * 场景调用
     * */
    public static Object myInvoke(final String methodName, final Object... ps)
    {
        return myPlusFunc.myInvoke(methodName, ps);
    }

    public static String superSql(String userToken, byte[] sql) {
        return myPlusFunc.superSql(userToken, sql);
    }

    public static String my_line_inary(byte[] bytes) {
        return (String)MyLineToBinary.restore(bytes);
    }

    public static Boolean hasConnPermission(String userToken) {
        return myPlusFunc.hasConnPermission(userToken);
    }
}
