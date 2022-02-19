namespace java org.apache.ignite.rpc.mythrift

/**
 * 连接
 * thrift -r --gen java /Users/chenfei/Documents/Java/MyGridGain/super-ignite/modules/core/src/main/resources/thrift/MySuperSqlFunc.thrift
*/
service MySuperSqlFunc
{
    /**
     * 获取 group id
     * @param userToken 用户token
     */
    i64 getGroupId(1: string userToken)
}