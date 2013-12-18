package com.hmsonline.memnon;

import com.hmsonline.memnon.MemnonService;


public class EmbeddableServer implements Runnable {
    String[] args = null;
    
    public EmbeddableServer(String[] args){
        this.args = args;        
    }

    public void run() {
        try {
            MemnonService.main(this.args);
        } catch (Exception e) {
            e.printStackTrace();
        }       
    }
}
