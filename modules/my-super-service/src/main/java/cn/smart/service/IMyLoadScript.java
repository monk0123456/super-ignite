package cn.smart.service;

/**
 * 加载 smart script
 * */
public interface IMyLoadScript {

    public String loadFromNative(final String path);

    public String loadFromHttp(final String url);

    public String loadCode(final String code);
}
