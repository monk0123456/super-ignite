package org.gridgain.myservice;

import cn.mysuper.service.IMyPlusFunc;
import cn.plus.model.ddl.MyFunc;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.scheduler.SchedulerFuture;
import org.gridgain.plus.dml.MyScenes;
import org.tools.MyPlusUtil;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

public class MyPlusFuncImpl implements IMyPlusFunc {
    @Override
    public String getScheduler(String schedulerName) {
        StringBuilder sb = new StringBuilder();
        SchedulerFuture schedulerFuture = MyPlusUtil.getIgniteScheduleProcessor(Ignition.ignite()).getScheduledFutures().get(schedulerName);
        sb.append("任务名称：" + schedulerName);
        sb.append(" 是否在运行：" + schedulerFuture.isRunning());
        sb.append(" 开始时间：" + schedulerFuture.createTime());
        sb.append(" 结束时间：" + schedulerFuture.lastFinishTime());
        return sb.toString();
    }

    @Override
    public Object myFun(String methodName, Object... ps) {
        Ignite ignite = Ignition.ignite();
        MyFunc myFunc = (MyFunc) ignite.cache("my_func").get(methodName);
        try {
            Class<?> cls = Class.forName(myFunc.getCls_name());
            Constructor constructor = cls.getConstructor();
            Object myobj = constructor.newInstance();
            Method[] methods = myobj.getClass().getMethods();
            for (Method method : methods)
            {
                if (method.getName().equals(myFunc.getJava_method_name()))
                {
                    Object rs = method.invoke(myobj, ps);
                    return rs;
                }
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Object myInvoke(String methodName, Object... ps) {
        ArrayList<Object> lst = new ArrayList<>();
        for (Object m : ps)
        {
            lst.add(m);
        }
        return MyScenes.superInvoke(Ignition.ignite(), methodName, lst);
    }
}
