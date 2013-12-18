package com.hmsonline.memnon;

import org.junit.AfterClass;
import org.junit.BeforeClass;

//import com.hmsonline.cassandra.triggers.dao.CommitLog;
//import com.hmsonline.cassandra.triggers.dao.ConfigurationStore;
//import com.hmsonline.cassandra.triggers.dao.TriggerStore;

public abstract class MemnonServerTest {
    static Thread serverThread = null;

    @BeforeClass
    public static void setup() throws Exception {
        serverThread = new Thread(new EmbeddableServer(new String[] { "server", "src/test/resources/virgil.yaml", "-host=localhost"}));
        serverThread.start();
        Thread.sleep(5000);
//        ConfigurationStore.getStore().create();
//        TriggerStore.getStore().create();
//        CommitLog.getCommitLog().create();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        serverThread.interrupt();
    }
}
