package cn.mysuper.model;

import java.io.Serializable;

public class MyUrlToken implements Serializable {
    private static final long serialVersionUID = 7939131112148132767L;

    private String userToken;
    private String url;

    public MyUrlToken(final String userToken, final String url)
    {
        this.url = url;
        this.userToken = userToken;
    }

    public MyUrlToken()
    {}

    public String getUserToken() {
        return userToken;
    }

    public void setUserToken(String userToken) {
        this.userToken = userToken;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}