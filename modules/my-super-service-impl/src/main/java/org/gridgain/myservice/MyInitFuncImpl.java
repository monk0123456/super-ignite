package org.gridgain.myservice;

import cn.mysuper.service.IInitFunc;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.ConnectionManager;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.gridgain.plus.init.PlusInit;

import java.util.logging.Logger;

public class MyInitFuncImpl implements IInitFunc {

    @Override
    public void initFunc() {
        Ignite ignite = Ignition.ignite();
        PlusInit plusInit = new PlusInit(ignite);
        plusInit.initialization();
        GridKernalContext ctx = ((IgniteEx)ignite).context();
        IgniteH2Indexing h2Indexing = (IgniteH2Indexing)ctx.query().getIndexing();
        ConnectionManager connMgr = h2Indexing.connections();
        System.out.println("自定义方法的初始化");

        try {
            String clause = "CREATE ALIAS IF NOT EXISTS auto_id FOR \"org.tools.MyPlusFunc.auto_id\"";
            connMgr.executeStatement("PUBLIC", clause);
            System.out.println("自定义方法：auto_id 初始化成功！");
            clause = "CREATE ALIAS IF NOT EXISTS my_fun FOR \"org.tools.MyPlusFunc.myFun\"";
            connMgr.executeStatement("PUBLIC", clause);
            System.out.println("自定义方法：my_fun 初始化成功！");
            clause = "CREATE ALIAS IF NOT EXISTS my_invoke FOR \"org.tools.MyPlusFunc.myInvoke\"";
            connMgr.executeStatement("PUBLIC", clause);
            System.out.println("自定义方法：my_invoke 初始化成功！");
            clause = "CREATE ALIAS IF NOT EXISTS nth FOR \"org.tools.MyPlusFunc.nth\"";
            connMgr.executeStatement("PUBLIC", clause);
            System.out.println("自定义方法：nth 初始化成功！");
            clause = "CREATE ALIAS IF NOT EXISTS first FOR \"org.tools.MyPlusFunc.first\"";
            connMgr.executeStatement("PUBLIC", clause);
            System.out.println("自定义方法：first 初始化成功！");
            clause = "CREATE ALIAS IF NOT EXISTS show_msg FOR \"org.tools.MyPlusFunc.showMsg\"";
            connMgr.executeStatement("PUBLIC", clause);
            System.out.println("自定义方法：show_msg 初始化成功！");
            clause = "CREATE ALIAS IF NOT EXISTS get_scheduler FOR \"org.tools.MyPlusFunc.getScheduler\"";
            connMgr.executeStatement("PUBLIC", clause);
            System.out.println("自定义方法：get_scheduler 初始化成功！");
            clause = "CREATE ALIAS IF NOT EXISTS hasConnPermission FOR \"org.tools.MyPlusFunc.hasConnPermission\"";
            connMgr.executeStatement("PUBLIC", clause);
            System.out.println("自定义方法：hasConnPermission 初始化成功！");
            clause = "CREATE ALIAS IF NOT EXISTS superSql FOR \"org.tools.MyPlusFunc.superSql\"";
            connMgr.executeStatement("PUBLIC", clause);
            System.out.println("自定义方法：superSql 初始化成功！");
            clause = "CREATE ALIAS IF NOT EXISTS my_line_binary FOR \"org.tools.MyPlusFunc.my_line_binary\"";
            connMgr.executeStatement("PUBLIC", clause);
            System.out.println("自定义方法：my_line_binary 初始化成功！（这个方法是用作测试的）");
        } catch (IgniteCheckedException var5) {
            var5.printStackTrace();
        }

    }

//    protected IgniteLogger log;
//    //private Logger LOG = Logger.getLogger(JdbcThinConnection.class.getName());
//
//    @Override
//    public void initFunc() {
//
//        GridKernalContext ctx = ((IgniteEx) Ignition.ignite()).context();
//        log = ctx.log(getClass());
//
//        IgniteH2Indexing h2Indexing = (IgniteH2Indexing)ctx.query().getIndexing();
//        ConnectionManager connMgr = h2Indexing.connections();
//
//        // 自定义方法的初始化
//        //ConnectionManager connMgr = ((IgniteH2Indexing)((IgniteEx) Ignition.ignite()).context().query().getIndexing()).connections();
//        try {
//            String clause = "CREATE ALIAS IF NOT EXISTS auto_id FOR \"org.tools.MyPlusFunc.auto_id\"";
//            connMgr.executeStatement("PUBLIC", clause);
//            log.info("自定义方法：auto_id 初始化成功！");
//
//            clause = "CREATE ALIAS IF NOT EXISTS my_fun FOR \"org.tools.MyPlusFunc.myFun\"";
//            connMgr.executeStatement("PUBLIC", clause);
//            log.info("自定义方法：my_fun 初始化成功！");
//
//            clause = "CREATE ALIAS IF NOT EXISTS my_invoke FOR \"org.tools.MyPlusFunc.myInvoke\"";
//            connMgr.executeStatement("PUBLIC", clause);
//            log.info("自定义方法：my_invoke 初始化成功！");
//
//            clause = "CREATE ALIAS IF NOT EXISTS nth FOR \"org.tools.MyPlusFunc.nth\"";
//            connMgr.executeStatement("PUBLIC", clause);
//            log.info("自定义方法：nth 初始化成功！");
//
//            clause = "CREATE ALIAS IF NOT EXISTS first FOR \"org.tools.MyPlusFunc.first\"";
//            connMgr.executeStatement("PUBLIC", clause);
//            log.info("自定义方法：first 初始化成功！");
//
//            clause = "CREATE ALIAS IF NOT EXISTS show_msg FOR \"org.tools.MyPlusFunc.showMsg\"";
//            connMgr.executeStatement("PUBLIC", clause);
//            log.info("自定义方法：show_msg 初始化成功！");
//
//            clause = "CREATE ALIAS IF NOT EXISTS get_scheduler FOR \"org.tools.MyPlusFunc.getScheduler\"";
//            connMgr.executeStatement("PUBLIC", clause);
//            log.info("自定义方法：get_scheduler 初始化成功！");
//        } catch (IgniteCheckedException e) {
//            e.printStackTrace();
//        }
//    }
}
