package com.griddelta.memnon.hadoop;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;

public class CodeLoader {
    public String loadCode(URL url) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = in.readLine()) != null) {
            sb.append(line);
        }
        in.close();
        return sb.toString();
    }
}
