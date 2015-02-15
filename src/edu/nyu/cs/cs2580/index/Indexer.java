package edu.nyu.cs.cs2580.index;

import edu.nyu.cs.cs2580.CorpusAnalyzer;
import edu.nyu.cs.cs2580.CorpusAnalyzerPagerank;
import edu.nyu.cs.cs2580.SearchEngine.Options;
import edu.nyu.cs.cs2580.helper.HtmlParser;
import edu.nyu.cs.cs2580.helper.ProgressBar;
import edu.nyu.cs.cs2580.models.Document;
import edu.nyu.cs.cs2580.models.DocumentIndexed;
import edu.nyu.cs.cs2580.models.QueryPhrase;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.spell.PlainTextDictionary;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This is the abstract Indexer class for all concrete Indexer implementations.
 * <p/>
 * Use {@link Indexer.Factory} to create concrete Indexer implementation.
 * Do NOT change the interface of this class.
 *
 * @author congyu
 * @author fdiaz
 *         <p/>
 *         12-01-2014 commented out HW3 logMiner code.
 */
public abstract class Indexer implements Runnable {

    protected static final String DOC_MAP_INDEX = "/document_map.idx.serialized";
    Map<Long, DocumentIndexed> indexFromDocId = new ConcurrentHashMap<Long, DocumentIndexed>();

    String outputFileName;
    File[] filesToIndex;
    int startIndex, endIndex;
    Map<String, Long> filenameToDocID;

    IndexOrganizer organizer;
    // Options to configure each concrete Indexer, do not serialize.
    protected Options _options = null;
    // CorpusAnalyzer and LogMinder that support the indexing process.
    protected CorpusAnalyzer _corpusAnalyzer = null;
    //protected edu.nyu.cs.cs2580.LogMiner _logMiner = null;

    // In-memory data structures populated once for each server. Those fields
    // are populated during index loading time and must not be modified during
    // serving unless they are made thread-safe. For comments, see APIs below.
    // Subclasses should populate those fields properly.
    protected int _numDocs = 0;
    protected long _totalTermFrequency = 0;

    private SpellChecker spellChecker;

    // Provided for serialization.
    public Indexer() {
    }

    // The real constructor
    public Indexer(Options options) {
        _options = options;
        _corpusAnalyzer = CorpusAnalyzer.Factory.getCorpusAnalyzerByOption(options);
        filenameToDocID = loadFilenameToDocID();

        //_logMiner = LogMiner.Factory.getLogMinerByOption(options);

    }

    /**
     * Iterator access to documents, used in HW2 for retrieving terms features for
     * the query matching the documents.
     *
     * @param query
     * @param docid
     * @return the next Document after {@code docid} satisfying {@code query} or
     * null if no such document exists.
     */
    public abstract Document nextDoc(QueryPhrase query, long docid);

    // Number of documents in the corpus.
    public final int numDocs() {
        return _numDocs;
    }

    // Number of term occurrences in the corpus. If a term appears 10 times, it
    // will be counted 10 times.
    public final long totalTermFrequency() {
        return _totalTermFrequency;
    }

    // Number of documents in which {@code term} appeared, over the full corpus.
    public abstract int corpusDocFrequencyByTerm(String term);

    // Number of times {@code term} appeared in corpus.
    public abstract int corpusTermFrequency(String term);

    // Number of times {@code term} appeared in the document {@code url}.
    public abstract int documentTermFrequency(String term, String url);

    /**
     * All Indexers must be created through this factory class based on the
     * provided {@code options}.
     */
    public static class Factory {
        public static Indexer getIndexerByOption(Options options) {
            if (options._indexerType.equals("inverted-compressed")) {
                return new IndexerInvertedCompressed(options);
            }
            return null;
        }
    }

    public SpellChecker getSpellChecker() {
        return spellChecker;
    }

    public void run() {
        System.out.println("Starting to run " + outputFileName);
        for (int i = startIndex; i < endIndex; i++) {
            File f = filesToIndex[i];
            if (f.isDirectory()) continue;
            long docId = filenameToDocID.get(f.getName());
            try {
                HtmlParser parser = new HtmlParser(f);
                processDocument(f.getAbsolutePath(),
                        docId,
                        parser.getTitle(),
                        parser.getBody());
                _totalTermFrequency += indexFromDocId.get(docId).size;
            } catch (Exception e) {
                System.out.println("error when processing doc:" + f.getName());
                e.printStackTrace();
            }
        }
        try {
            flushIndexToFile(outputFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Done running for " + outputFileName);
    }


    private Map<String, Long> loadFilenameToDocID() {
        Map<String, Long> filenameToDocID = null;
        try {
            FileInputStream reader = new FileInputStream(_options._indexPrefix + "/" + _options._filename_to_DocId);
            ObjectInputStream input = new ObjectInputStream(reader);
            filenameToDocID = (Map<String, Long>) input.readObject();
            input.close();
            reader.close();
        } catch (Exception e) {
            try {
                filenameToDocID = generateDocIDs();    // If they don't exist, generate them!
                flushDocIDToFile();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return filenameToDocID;
    }

    public boolean removeFromCache(String token) {
        return organizer.removeFromCache(token);
    }

    public DocumentIndexed getDoc(int docid) {
        return indexFromDocId.get((long) docid);
    }

    /**
     * Assigns docid to each document.
     * Each document should be a file within the passed 'filelist'
     *
     * @throws IOException
     */
    protected Map<String, Long> generateDocIDs() throws IOException {
        File[] filelist = new File(_options._corpusPrefix).listFiles();
        ProgressBar pb = new ProgressBar();
        long docId = 1;

        // generate docids for all files
        System.out.println("Generating docids...");
        int count = 0;
        if (filenameToDocID == null) filenameToDocID = new ConcurrentHashMap<String, Long>();
        for (File f : filelist) {
            if (!filenameToDocID.containsKey(f.getName())) {
                filenameToDocID.put(f.getName(), docId);
                docId++;
            }
            pb.update(count++, filelist.length);
        }
        return filenameToDocID;
    }

    /**
     * Writes Document names (filenames) to doc id map to file.
     */
    private void flushDocIDToFile() {
        String pathstr = _options._indexPrefix + "/";
        String filename = pathstr + _options._filename_to_DocId;

        // make sure directory exists
        File newFile = new File(pathstr);
        if (!newFile.exists()) {
            newFile.mkdirs();
        }

        try {
            FileOutputStream newGraph = new FileOutputStream(filename);
            System.out.println("Writing docids to " + filename);
            ObjectOutputStream output = new ObjectOutputStream(newGraph);
            output.writeObject(filenameToDocID);
            output.close();
            newGraph.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    abstract void flushIndexToFile(String filename) throws IOException;

    abstract void processDocument(String filePath, long docId, String title, String body) throws IOException;

    public void loadIndex() throws IOException, ClassNotFoundException {
        String indexDirectory = _options._indexPrefix;
        System.out.println("Load index from: " + indexDirectory);

        File pre_index_file = new File(_options._indexPrefix + DOC_MAP_INDEX);
        ObjectInputStream reader = new ObjectInputStream(
                new FileInputStream(pre_index_file));
        indexFromDocId = (Map<Long, DocumentIndexed>) reader.readObject();
        reader.close();

        _numDocs = indexFromDocId.size();

        _totalTermFrequency = 0;
        for (DocumentIndexed doc : indexFromDocId.values()) {
            _totalTermFrequency += doc.size;
        }

        spellChecker = new SpellChecker(FSDirectory.open(new File("spellchecker")));
        spellChecker.indexDictionary(new PlainTextDictionary(new File(IndexOrganizer.DICTIONARY_FILENAME)), new IndexWriterConfig(Version.LUCENE_36, new EnglishAnalyzer(Version.LUCENE_36)), true);

        System.out.println("Indexed " + _numDocs + " docs with " + _totalTermFrequency + " terms.");
    }

    /**
     * Loop over every document in the corpus directory, adding it to the index.
     * Every DOCS_PER_GROUP, a new index is started.
     * Once all documents are finished indexing, the different indices created
     * for groups of docs are merged together into one large index with no
     * repeated values.
     * <p/>
     * To write the current set of documents' index out to a file, call
     * flushIndexToFile().
     *
     * @throws java.io.IOException
     */
    public void constructIndex() throws IOException {
        Map<String, Long> filenameToDocID = loadFilenameToDocID();

        Map<Long, Float> docIDToPageRank = (Map<Long, Float>) new CorpusAnalyzerPagerank(_options).load();
        System.out.println("Loaded " + docIDToPageRank.size() + " page ranks.");

        File docDirectory = new File(_options._corpusPrefix);
        System.out.println("Construct Inverted index from: " + docDirectory.getPath());
        _totalTermFrequency = 0;
        File[] files = docDirectory.listFiles();
        _numDocs = files.length;
        ExecutorService executorService = Executors.newFixedThreadPool(_options._num_indexing_threads);
        List<Indexer> indexerThreads = new ArrayList<Indexer>();

        double time = System.currentTimeMillis();
        for (int i = 0; i <= _numDocs / _options._docs_per_thread; i++) {
            Indexer indexerThread = new IndexerInvertedCompressed(
                    _options,
                    filenameToDocID,
                    IndexOrganizer.DOC_INDEX_PREFIX + i,
                    files,
                    i * _options._docs_per_thread,
                    Math.min((i + 1) * _options._docs_per_thread, _numDocs),
                    indexFromDocId,
                    docIDToPageRank);
            indexerThreads.add(indexerThread);
            executorService.execute(indexerThread);
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            System.err.println("Threads timed out!!");
            e.printStackTrace();
        }

        time = (System.currentTimeMillis() - time) / 1000.0;

        System.out.println("All threads finished in " + time + " seconds!");
        for (Indexer t : indexerThreads) {
            _totalTermFrequency += t._totalTermFrequency;
        }

        System.out.println("Indexed " + _numDocs + " docs with " + _totalTermFrequency + " terms.");

        String indexFile = _options._indexPrefix + DOC_MAP_INDEX;
        System.out.println("Store doc map to: " + indexFile);
        ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(indexFile));
        writer.writeObject(indexFromDocId);
        writer.close();

        organizer.mergeAndSplit();
    }

    public abstract byte[] encode(long[] postingList);
}
