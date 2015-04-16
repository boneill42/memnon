package com.hmsonline.memnon;

import com.griddelta.memnon.MemnonApplication;


public class EmbeddableServer implements Runnable {
    String[] args = null;
    
    public EmbeddableServer(String[] args){
        this.args = args;        
    }

    public void run() {
        try {
            MemnonApplication.main(this.args);
        } catch (Exception e) {
            e.printStackTrace();
        }       
    }
}
