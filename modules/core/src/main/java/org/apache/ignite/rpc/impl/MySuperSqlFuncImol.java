package org.apache.ignite.rpc.impl;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.rpc.mythrift.MySuperSqlFunc;
import org.apache.thrift.TException;

import java.io.Serializable;
import java.util.List;

public class MySuperSqlFuncImol implements MySuperSqlFunc.Iface, Serializable {
    private static final long serialVersionUID = 2855533444976426318L;

    private Ignite ignite;

    public MySuperSqlFuncImol(final Ignite ignite)
    {
        this.ignite = ignite;
    }

    @Override
    public long getGroupId(String userToken) throws TException {
        if (userToken != null && ignite.configuration().getRoot_token().equals(userToken))
        {
            return 0L;
        }
        else
        {
            List<List<?>> lst_row = ignite.cache("my_users_group").query(new SqlFieldsQuery("select g.id from my_users_group as g where g.user_token = ?").setArgs(userToken)).getAll();
            for (List<?> row : lst_row)
            {
                if (row.get(0) != null)
                {
                    return Long.parseLong(row.get(0).toString());
                }
            }
        }
        return -1L;
    }
}
