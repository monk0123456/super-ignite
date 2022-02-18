package org.apache.ignite.rpc.service;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.rpc.impl.MySuperSqlFuncImol;
import org.apache.ignite.rpc.mythrift.MySuperSqlFunc;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MySuperSqlFuncService {

    private Ignite ignite;
    private MySuperSqlFuncImol mySuperSqlFuncImol;
    private ExecutorService singlePool;
    private TServer server = null;

    public MySuperSqlFuncService(final Ignite ignite) throws Exception {
        this.ignite = ignite;
        this.mySuperSqlFuncImol = new MySuperSqlFuncImol(ignite);
        this.singlePool = Executors.newSingleThreadExecutor();

        int port = 9090;

        // 设置传输通道，普通通道
        TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(port);
        // 使用高密度二进制协议
        TProtocolFactory proFactory = new TCompactProtocol.Factory();
        // 设置处理器 MyMetaServiceImpl
        TProcessor processor = new MySuperSqlFunc.Processor(mySuperSqlFuncImol);

        TNonblockingServer.Args tnbargs = new TNonblockingServer.Args(serverTransport);
        tnbargs.processor(processor);
        tnbargs.transportFactory(new TFramedTransport.Factory());
        tnbargs.protocolFactory(proFactory);

        // 使用非阻塞式IO，服务端和客户端需要指定TFramedTransport数据传输的方式
        this.server = new TNonblockingServer(tnbargs);
    }

    public void execute() throws Exception {
        if (singlePool != null && server != null)
        {
            singlePool.execute(new Runnable() {
                @Override
                public void run() {
                    X.println("数据服务启动 ...");
                    server.serve();
                }
            });
        }
    }
}
