package cn.mysuper.service;

/**
 * 自定义方法
 * */
public interface IMyPlusFunc {

    public Boolean hasConnPermission(String var1);

    /**
     * 获取定时任务详细信息
     * */
    public String getScheduler(String schedulerName);

    /**
     * 自定义函数的要求
     * 1、必须有一个默认构造函数
     * 2、方法名不能重载
     * */
    public Object myFun(final String methodName, final Object... ps);

    /**
     * 场景调用
     * */
    public Object myInvoke(final String methodName, final Object... ps);

    public String superSql(final String var1, final byte[] var2);
    //public String superSql(final String userToken, final String sql);
}
