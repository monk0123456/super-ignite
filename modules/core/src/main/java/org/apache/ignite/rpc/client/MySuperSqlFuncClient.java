package org.apache.ignite.rpc.client;

import org.apache.ignite.rpc.mythrift.MySuperSqlFunc;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class MySuperSqlFuncClient {

    public static long getGroupId(final String userToken)
    {
        //U.getIgniteHome();
        int port = 9090;
        TTransport transport = new TFramedTransport(new TSocket("127.0.0.1", port));
        TProtocol protocol = new TCompactProtocol(transport);
        MySuperSqlFunc.Client client = new MySuperSqlFunc.Client(protocol);

        long result = -1L;
        try {
            transport.open();
            result = client.getGroupId(userToken);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            transport.close();
        }
        return result;
    }
}
