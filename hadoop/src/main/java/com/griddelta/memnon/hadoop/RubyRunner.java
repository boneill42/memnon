package com.griddelta.memnon.hadoop;

import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.jruby.RubyHash;
import org.jruby.embed.EmbedEvalUnit;
import org.jruby.embed.LocalContextScope;
import org.jruby.embed.LocalVariableBehavior;
import org.jruby.embed.ScriptingContainer;
import org.jruby.util.JRubyClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;

public class RubyRunner {
    private static final Logger LOG = LoggerFactory.getLogger(RubyRunner.class);

    private static ScriptingContainer RUBY_CONTAINER;
    private EmbedEvalUnit script;
    private static final Object MUTEX = new Object();

    public RubyRunner(String rubyCode, List<URL> supportSearchPaths) {
        LOG.info("Instantiating RubyRunner with [{}]", rubyCode.hashCode());
        getRubyContainer().setCompatVersion(org.jruby.CompatVersion.RUBY1_9);
        if (supportSearchPaths != null) {
            JRubyClassLoader cl = getRubyContainer().getProvider().getRuntime().getJRubyClassLoader();
            for (URL url : supportSearchPaths) {
                cl.addURL(url);
            }
        }
        script = getRubyContainer().parse(rubyCode);
        script.run();
    }

    private ScriptingContainer getRubyContainer() {
        if (RUBY_CONTAINER == null) {
            synchronized (MUTEX) {
                if (RUBY_CONTAINER == null) {
                    RUBY_CONTAINER = new ScriptingContainer(LocalContextScope.CONCURRENT,
                            LocalVariableBehavior.TRANSIENT, true);
                    // CONCURRENT is weird, it's not ready for using after the
                    // construction. Need to wait until the Runtime is ready...
                    while (RUBY_CONTAINER.getProvider().getRuntime() == null) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        return RUBY_CONTAINER;
    }

    public Object runMap(Row row) {
        return RUBY_CONTAINER.callMethod(null, "map", row);
    }

    // TODO: Originally had it so the result was an array of two arrays, first containing keys, second containing values.
    //       Maybe that is better, because it allows duplicating keys??? but I like hashes better. =)
    @SuppressWarnings("unchecked")
    public Map<Object, Object> runReduce(String key, Iterable<Text> values) {
        RubyHash output = (RubyHash) RUBY_CONTAINER.callMethod(null, "reduce", key, values);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Result = [" + output.getClass() + "] : [" + output + "]");
        }
        return output;
    }
}
