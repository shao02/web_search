package edu.nyu.cs.cs2580.generator;
import edu.nyu.cs.cs2580.helper.ProgressBar;

import java.io.File;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static edu.nyu.cs.cs2580.CorpusAnalyzer.*;

/**
 * Created by chenprice on 12/5/14.
 */
public class GraphGenerator implements Runnable {
    final int CHECKPOINT = 50;
    ConcurrentHashMap<String, Long> docidByFilename;
    ConcurrentHashMap<Long, TreeSet<Long>> linkGraph;
    ConcurrentHashMap<Long, Float> startRanks;
    ConcurrentHashMap<Long, Float> finalRanks;
    List<File> filelist;
    int numFilesTotal;
    int threadId;

    public GraphGenerator(List<File> filelist, ConcurrentHashMap<String, Long> docidByFilename,
                          ConcurrentHashMap<Long, TreeSet<Long>> linkGraph,
                          ConcurrentHashMap<Long, Float> startRanks,
                          ConcurrentHashMap<Long, Float> finalRanks,
                          int threadId,
                          int numFilesTotal){
        this.docidByFilename = docidByFilename;
        this.linkGraph = linkGraph;
        this.startRanks = startRanks;
        this.finalRanks = finalRanks;
        this.filelist = filelist;
        this.numFilesTotal = numFilesTotal;
        this.threadId = threadId;
    }

    private void extractLinks() {
        System.out.println("Thread " + threadId + " started.");
        for (File f : filelist) {
            if (f.isDirectory()) continue;
            try {
                String currSource = null;
                TreeSet<Long> currList = null;
                long currId = -1;

                if (isValidDocument(f)) {
                    currList = new TreeSet<Long>();
                    HeuristicLinkExtractor linkExtractor = new HeuristicLinkExtractor(f);
                    currSource = linkExtractor.getLinkSource();
                    String currLink = linkExtractor.getNextInCorpusLinkTarget();
                    while (currLink != null) {
                        if (docidByFilename.containsKey(currLink)) {
                            currList.add(docidByFilename.get(currLink));
                        }
                        currLink = linkExtractor.getNextInCorpusLinkTarget();
                    }
                    currId = docidByFilename.get(currSource);
                    linkGraph.put(currId, currList);
                    startRanks.put(currId, 0.0f);
                    finalRanks.put(currId, 0.0f);
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
        extractLinks();
    }

}
