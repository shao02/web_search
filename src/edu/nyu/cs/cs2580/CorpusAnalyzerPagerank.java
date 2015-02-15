package edu.nyu.cs.cs2580;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import edu.nyu.cs.cs2580.SearchEngine.Options;
import edu.nyu.cs.cs2580.generator.GraphGenerator;
import edu.nyu.cs.cs2580.generator.IdGenerator;
import edu.nyu.cs.cs2580.helper.*;

/**
 * A class that mines the corpus documents (web pages), generates doc id for each web page,
 * builds link graph, and computes a PageRank value for each web page.
 *
 */
public class CorpusAnalyzerPagerank extends CorpusAnalyzer {
    private static final float LAMBDA = 0.9f;
    private static final int ITERATIONS = 2;
    private static final float LAMBDA_COMP = 1 - LAMBDA;
    private static String PAGERANK_FILENAME = "pageranks_lambda09_twoIterations";
    int _numDocs = 0;
    long docid = 1;
    ConcurrentHashMap<String, Long> docidByFilename;
    ConcurrentHashMap<Long, TreeSet<Long>> linkGraph = new ConcurrentHashMap<Long, TreeSet<Long>>(16, 0.9f, 2);
    ConcurrentHashMap<Long, Float> startRanks = new ConcurrentHashMap<Long, Float>();
    ConcurrentHashMap<Long, Float> finalRanks = new ConcurrentHashMap<Long, Float>();
    int numPrThreads = _options._num_mining_threads;

    public CorpusAnalyzerPagerank(Options options) {
        super(options);
        docidByFilename = new ConcurrentHashMap<String, Long>(16, 0.9f, 2);
    }

    /**
     * This function processes the corpus as specified inside {@link _options}
     * and extracts the "internal" graph structure from the pages inside the
     * corpus. Internal means we only store links between two pages that are both
     * inside the corpus.
     * <p/>
     *
     * @throws IOException
     */
    @Override
    public void prepare() throws IOException {
        System.out.println("Preparing " + this.getClass().getName());
        File docDirectory = new File(_options._corpusPrefix);
        File[] filelist = docDirectory.listFiles();

        // split file list
        ArrayList<List<File>> filelists = new ArrayList<List<File>>();
        List<File> filenamesAsList = Arrays.asList(filelist);
        //int numFilesPerThread = filelist.length / numPrThreads;
        int numFilesPerThread = _options._docs_per_thread;
        int startFile = 0;
        for (int i = 0; i <= filelist.length / numFilesPerThread; i++) {
            int endFile = Math.min((i + 1) * numFilesPerThread, filelist.length);
            filelists.add(filenamesAsList.subList(startFile, endFile));
            startFile += numFilesPerThread;
        }

        // generate doc id
        double time = System.currentTimeMillis();
        generateId(filelists, filelist.length);
        time = (System.currentTimeMillis() - time) / 1000.0;
        System.out.println("All threads finished in " + time + " seconds!");

        // generate link graph
        time = System.currentTimeMillis();
        generateLinkGraph(filelists, filelist.length);
        time = (System.currentTimeMillis() - time) / 1000.0;
        System.out.println("All threads finished in " + time + " seconds!");
        _numDocs = linkGraph.size();
        System.out.println("link graph size: " + linkGraph.size() + " nodes.");
        return;
    }

    /**
     * This function computes the PageRank based on the internal graph generated
     * by the {@link prepare} function, and stores the PageRank to be used for
     * ranking.
     * <p/>
     * Note that you will have to store the computed PageRank with each document
     * the same way you do the indexing for HW2. I.e., the PageRank information
     * becomes part of the index and can be used for ranking in serve mode. Thus,
     * you should store the whatever is needed inside the same directory as
     * specified by _indexPrefix inside {@link _options}.
     *
     * @throws IOException
     */
    @Override
    public void compute() throws IOException {
        ProgressBar pb = new ProgressBar();
        System.out.println("Computing using " + this.getClass().getName());

        // set initial PR
        float init = 1.0f / _numDocs;
        for (Long i : startRanks.keySet()) {
            startRanks.put(i, LAMBDA_COMP * init);
        }

        // iterate using PR algorithm
        int count = 0;
        int itercount = ITERATIONS;
        while (itercount > 0) {
            for (Long i : finalRanks.keySet()) {
                finalRanks.put(i, LAMBDA_COMP * init);
            }

            for (Long did : linkGraph.keySet()) {
                TreeSet<Long> currOutLinks = linkGraph.get(did);
                if (currOutLinks.size() > 0) {
                    for (Long q : currOutLinks) {
                        float currRank = finalRanks.get(q);
                        float newRank = currRank + LAMBDA *
                                startRanks.get(q) / (float) currOutLinks.size();
                        finalRanks.put(q, newRank);
                    }
                } else {
                    float currRank = finalRanks.get(did);
                    float newRank = currRank + LAMBDA_COMP *
                            startRanks.get(did) * init;
                    finalRanks.put(did, newRank);
                }
                pb.update(count++, _numDocs * ITERATIONS);
            }

            startRanks = finalRanks;
            itercount--;
        }

        // save data to disk
        flushPageRankToFile();
        flushDocidToFile();
        return;

    }

    /**
     * During indexing mode, this function loads the PageRank values computed
     * during mining mode to be used by the indexer.
     *
     * @throws IOException
     */
    @Override
    public Object load() throws IOException {
        System.out.println("Loading using " + this.getClass().getName());
        try {
            String pathstr = _options._pagerankPrefix + "/";
            String filename = pathstr + PAGERANK_FILENAME;
            FileInputStream reader = new FileInputStream(filename);
            ObjectInputStream input = new ObjectInputStream(reader);
            ConcurrentHashMap<Long, Float> recoveredPageRanks = (ConcurrentHashMap<Long, Float>) input.readObject();
            input.close();
            reader.close();
            return recoveredPageRanks;
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }


    /**
     * Concurrent ID generation
     */
    private void generateId(ArrayList<List<File>> filelists, int numFilesTotal) {
        System.out.println("Generating document ID.");
        int threadId = 1;
        int numIdThreads = Math.min(filelists.size(), _options._num_mining_threads);
        ExecutorService executorServiceId = Executors.newFixedThreadPool(numIdThreads);
        for (List<File> lf : filelists) {
            IdGenerator idGenerator = new IdGenerator(lf,
                    docidByFilename,
                    docid,
                    threadId++,
                    numFilesTotal);
            docid += lf.size();
            executorServiceId.execute(idGenerator);
        }
        executorServiceId.shutdown();
        try {
            executorServiceId.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            System.err.println("Id generator threads timed out!!");
            e.printStackTrace();
        }
        System.out.println("Done generating " + docidByFilename.size() + " document IDs.");
    }

    /**
     * Concurrent construction of webpage link graph, in preparation
     * for PageRank computation.
     */
    private void generateLinkGraph(ArrayList<List<File>> filelists, int numFilesTotal) {
        System.out.println("Generating link graph.");
        int threadId = 1;
        ExecutorService executorServicePr = Executors.newFixedThreadPool(numPrThreads);
        for (List<File> lf : filelists) {
            GraphGenerator gg = new GraphGenerator(lf,
                    docidByFilename,
                    linkGraph,
                    startRanks,
                    finalRanks,
                    threadId++,
                    numFilesTotal);
            executorServicePr.execute(gg);
        }
        executorServicePr.shutdown();
        try {
            executorServicePr.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            System.err.println("Graph generator timed out!!");
            e.printStackTrace();
        }
        System.out.println("Done generating link graph.");
    }

    /**
     * Writes page-rank scores object to file
     */
    private void flushPageRankToFile() {
        String pathstr = _options._pagerankPrefix + "/";
        String filename = pathstr + PAGERANK_FILENAME;

        // make sure directory exists
        File newFile = new File(pathstr);
        if (!newFile.exists()) {
            newFile.mkdirs();
        }

        try {
            FileOutputStream newGraph = new FileOutputStream(filename);
            System.out.println("Writing pageranks to " + filename);
            ObjectOutputStream output = new ObjectOutputStream(newGraph);
            output.writeObject(finalRanks);
            output.close();
            newGraph.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Writes Document names (filenames) to doc id map to file.
     */
    private void flushDocidToFile() {
        String pathstr = _options._indexPrefix + "/";
        String filename = pathstr + _options._filename_to_DocId;

        // make sure directory exists
        File newFile = new File(pathstr);
        if (!newFile.exists()) {
            newFile.mkdirs();
        }

        try {
            FileOutputStream newGraph = new FileOutputStream(filename);
            System.out.println("Writing docid's to " + filename);
            ObjectOutputStream output = new ObjectOutputStream(newGraph);
            output.writeObject(docidByFilename);
            output.close();
            newGraph.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
