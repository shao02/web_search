package edu.nyu.cs.cs2580.generator;

import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static edu.nyu.cs.cs2580.CorpusAnalyzer.*;

/**
 * Created by chenprice on 12/6/14.
 */
public class IdGenerator implements Runnable {
    List<File> filelist;
    ConcurrentHashMap<String, Long> docidByFilename;
    long docid;
    int numFilesTotal;
    int threadId;

    public IdGenerator(List<File> filelist,
                       ConcurrentHashMap<String, Long> docidByFilename,
                       long docid,
                       int threadId,
                       int numFilesTotal){
        this.filelist = filelist;
        this.docidByFilename = docidByFilename;
        this.docid = docid;
        this.numFilesTotal = numFilesTotal;
        this.threadId = threadId;
    }

    /**
     * Assigns docid to each document.
     * Each document should be a file within the passed 'filelist'
     */
    private void generateDocid(){
        System.out.println("Thread " + threadId + " started.");
        for (File f : filelist) {
            if (f.isDirectory()) continue;
            try {
                String currSource = f.getName();
                if (!docidByFilename.containsKey(currSource) && isValidDocument(f)) {
                    docidByFilename.put(currSource, docid);
                    docid++;
                }
            } catch (Exception e) {
                System.out.println("error when processing doc:" + f.getName());
                System.out.println(e.toString());
                e.printStackTrace();
            }
        }
        System.out.println("Thread " + threadId + " finished.");
    }

    @Override
    public void run(){
        generateDocid();
    }
}
