package cn.log;

import com.google.common.base.Charsets;
import com.google.common.io.CharSink;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.log4j.FileAppender;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MyLogger {

    private static class InstanceHolder
    {
        public static MyLogger instance = new MyLogger();
    }

    public static MyLogger getInstance() {
        return MyLogger.InstanceHolder.instance;
    }

    public void myWriter(final String txt) {
        try
        {
//            File f = new File(this.getClass().getResource("").getPath());
//            String path = f.getCanonicalPath();
//            System.out.println(path);
//            path = path + "/my_super_con.log";
            String path = "/Users/chenfei/Documents/Java/MyGridGainServer/my_log/my_super_con.log";

            List<String> lst = new ArrayList<>();
            lst.add(txt);
            //CharSink sink = Files.asCharSink(new File("src/main/resources/sample.txt"), Charsets.UTF_8, FileWriteMode.APPEND);
            CharSink sink = Files.asCharSink(new File(path), Charsets.UTF_8, FileWriteMode.APPEND);
            sink.writeLines(lst.stream());
        }
        catch (IOException e)
        {}
    }
}
