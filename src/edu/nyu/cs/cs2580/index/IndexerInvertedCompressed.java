package edu.nyu.cs.cs2580.index;

import edu.nyu.cs.cs2580.SearchEngine;
import edu.nyu.cs.cs2580.SearchEngine.Options;
import edu.nyu.cs.cs2580.helper.Compressor;
import edu.nyu.cs.cs2580.models.Document;
import edu.nyu.cs.cs2580.models.DocumentIndexed;
import edu.nyu.cs.cs2580.models.QueryPhrase;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class IndexerInvertedCompressed extends Indexer {

    protected static final int PHRASES_TO_CACHE = 3;
    Map<Long, Float> docIDToPageRank;

    protected IndexerInvertedCompressed(
            SearchEngine.Options options,
            Map<String, Long> filenameToDocID,
            String outputFileName,
            File[] files,
            int s,
            int e,
            Map<Long, DocumentIndexed> indexFromDocId,
            Map<Long, Float> docIDToPageRank) {
        super(options);
        this.outputFileName = outputFileName;
        this.filesToIndex = files;
        this.filenameToDocID = filenameToDocID;
        this.organizer = new IndexOrganizer(options, this);
        startIndex = s;
        endIndex = e;
        this.indexFromDocId = indexFromDocId;
        this.docIDToPageRank = docIDToPageRank;
    }

    LinkedHashMap<String, TreeMap<Long, ArrayList<Long>>> cachedPhrases =
            new LinkedHashMap<String, TreeMap<Long, ArrayList<Long>>>(10, .5f, true) {
        @Override
        public boolean removeEldestEntry(Map.Entry eldest){
            return size() > PHRASES_TO_CACHE;   //size exceeded the max allowed
        }
    };

    Map<String, TreeMap<Long, ArrayList<Long>>> indexFromToken = new ConcurrentHashMap<String, TreeMap<Long, ArrayList<Long>>>();
    public IndexerInvertedCompressed(Options options) {
        super(options);
        organizer = new IndexOrganizer(options, this);
        System.out.println("Using Indexer: " + this.getClass().getSimpleName());
    }

    public byte[] encode(long[] postingList) {
        return Compressor.convertToVBytes(Compressor.deltaEncode(postingList));
    }

    @Override
    public int corpusDocFrequencyByTerm(String term) {
        getDocsWithToken(term);

        Map<Long, ArrayList<Long>> map = indexFromToken.get(term);

        if (map == null) return 0;
        else return map.size();
    }

    @Override
    public int corpusTermFrequency(String term) {
        getDocsWithToken(term);
        Map<Long, ArrayList<Long>> map = indexFromToken.get(term);
        if (map == null) return 0;
        int frequency = 0;
        for (Long doc : map.keySet()) frequency += map.get(doc).size();
        return frequency;
    }

    @Override
    public int documentTermFrequency(String term, String url) {
        getDocsWithToken(term);

        int i = 0;
        Map<Long, ArrayList<Long>> map = indexFromToken.get(term);
        if (map == null) return 0;
        for (Long docId : map.keySet()) {
            if (indexFromDocId.get(docId).getUrl().equals(url)) return map.get(docId).size();
        }
        return i;
    }
    
    private String phrase_token="";
    static TreeSet<String> phraseRank = new TreeSet<String>();
  


    @Override
    protected void processDocument(String url, long docId, String title, String body) throws IOException {
        if (indexFromToken == null) indexFromToken = new ConcurrentHashMap<String, TreeMap<Long, ArrayList<Long>>>();
        StringTokenizer st = new StringTokenizer(body);
        long i = 0;
        ConcurrentHashMap<String, Integer> local_phraseRank= new ConcurrentHashMap<String, Integer>();
        
        int count = 0;
        int phrase_window=0;
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            // If we haven't seen this token before.
            if (indexFromToken.get(token) == null) indexFromToken.put(token, new TreeMap<Long, ArrayList<Long>>());

            // If we have seen this docId for this token before.
            if (indexFromToken.get(token).get(docId) == null) indexFromToken.get(token).put(docId, new ArrayList<Long>());
            indexFromToken.get(token).get(docId).add(i++);
            
            phrase_token+=token.toLowerCase()+' ';
          
            if(phrase_window==2){
            	if(local_phraseRank==null) local_phraseRank = new ConcurrentHashMap<String, Integer>();
            	 if (local_phraseRank.containsKey(phrase_token)){
                 	local_phraseRank.put(phrase_token, local_phraseRank.get(phrase_token)+1);
                 }	
                 else{
                 	local_phraseRank.put(phrase_token,0);
                 }  
            	phrase_token=phrase_token.substring(phrase_token.indexOf(' ')+1);//remove the first word from the window.
            	phrase_window=phrase_window-1;
            }
            
            if(phrase_window==1){ // move the phase to the local phraseRank.
                if (local_phraseRank.containsKey(phrase_token)){
                	local_phraseRank.put(phrase_token, local_phraseRank.get(phrase_token)+1);
                }	
                else{
                	local_phraseRank.put(phrase_token,0);
                }      	
            }
            phrase_window++;                                  
        }
        
        for(String phrase : local_phraseRank.keySet()){
    		
   		 if(local_phraseRank.get(phrase)>4 
   				 &&
   				 !phrase.matches(".*\\d+.*")
   				 &&
   				 !phrase.matches(".*to.*")
   				  &&
   				 !phrase.matches(".*of.*")
   				   &&
   				 !phrase.matches(".*is.*")
   				  &&
   				 !phrase.matches(".*be.*")
   				 &&
   				 !phrase.matches(".*have.*")
   				 &&
   				 !phrase.matches(".*with.*")
   				 &&
   				 !phrase.matches(".*on.*")
   				 &&
   				 !phrase.matches(".*in.*")
   				  &&
   				 !phrase.matches(".*can.*")
   				  &&
   				 !phrase.matches(".*by.*")
   				   &&
   				 !phrase.matches(".*than.*")
   				  &&
   				 !phrase.matches(".*from.*")
   				 &&
   				 !phrase.matches(".*about.*")
   				 &&
   				 !phrase.matches(".*http.*")
   				  &&
   				 !phrase.matches(".*as.*")
   				 &&
   				 !phrase.matches(".*which.*")
   				 ){
   			// System.out.println("phrase: "+phrase + " num:"+ local_phraseRank.get(phrase));		
           	 phraseRank.add(phrase);
            }
   	 }   
   	 local_phraseRank=null;
        
        DocumentIndexed doc = new DocumentIndexed(url, docId, i, title);
        doc.setPageRank(docIDToPageRank.get(docId));
        indexFromDocId.put(docId, doc);
    }

    /**
     * Take the current posting list and write it out to a file.
     *
     * Every line will be:
     *    token (docId numberOfOccurrences index+)+
     * @throws IOException
     */
    protected void flushIndexToFile(String outputFileName) throws IOException {
        if (indexFromToken != null) {
            TreeMap<String, List<Long>> map = new TreeMap<String, List<Long>>();
            Set<String> tokens = new HashSet<String>(indexFromToken.keySet());
            for (String token : tokens) {
                map.put(token, buildPostingList(indexFromToken.get(token)));
            }
            organizer.write(map, outputFileName);
            indexFromToken = null;
        }
    }

    /**
     * Create the standard posting list format given a Map from DocId to a list of
     * indices. The format is:
     *    "[docId] [# of occurrences] {list of occurrences}"
     * @param occurrences
     * @return
     */
    protected List<Long> buildPostingList(TreeMap<Long, ArrayList<Long>> occurrences) {
        List<Long> result = new ArrayList<Long>();
        List<Long> docs = new ArrayList<Long>(occurrences.keySet());
        Collections.sort(docs);
        for (Long docId : docs) {
            ArrayList<Long> indices = occurrences.get(docId);
            result.add(docId);
            result.add((long) indices.size());
            result.addAll(indices);
        }
        return result;
    }

    @Override
    public DocumentIndexed nextDoc(QueryPhrase query, long docid) {
        List<Long> docsWithAllTokens = new ArrayList<Long>(getDocsWithTokens(query._tokens));
        Collections.sort(docsWithAllTokens);
        int i = Collections.binarySearch(docsWithAllTokens, (long) docid);

        // If we didn't find this docId in the list, this is the negative of the
        // index of the first document with higher id.
        if (i < 0) {
            i *= -1;
            if (i >= docsWithAllTokens.size()) return null;
            else return indexFromDocId.get(docsWithAllTokens.get(i - 1));
        }

        // If we found the actual doc id, return the one after it.
        if (i + 1 >= docsWithAllTokens.size()) return null;

        return indexFromDocId.get(docsWithAllTokens.get(i + 1));
    }

    private Set<Long> getDocsWithTokens(Iterable<String> tokens) {
        List<Set<Long>> docsWithToken = new ArrayList<Set<Long>>();
        for (String token : tokens) docsWithToken.add(getDocsWithToken(token));
        return intersection(docsWithToken);
    }

    private Set<Long> intersection(List<Set<Long>> sets) {
        Set<Long> result = new HashSet<Long>();
        for (Set<Long> set : sets) {
            if (set != null) {
                if (result.isEmpty()) result.addAll(set);
                else {
                    Set<Long> newResult = new HashSet<Long>(result);
                    for (Long s : result) {
                        if (!set.contains(s)) newResult.remove(s);
                    }
                    result = newResult;
                }
            }
        }
        return result;
    }

    private Set<Long> getDocsWithToken(String token) {
        if (token.contains(" ")) {
            TreeMap<Long, ArrayList<Long>> map = getDocumentsWithPhrase(token);
            if (indexFromToken.get(token) == null) indexFromToken.put(token, new TreeMap<Long, ArrayList<Long>>());
            indexFromToken.get(token).putAll(map);
            return map.keySet();
        }
        else {
            buildIndexFor(token, organizer.loadIndexFor(token));
            if (indexFromToken.containsKey(token)) return indexFromToken.get(token).keySet();
            else return null;
        }
    }

    public void buildIndexFor(String token, byte[] bytes) {
        if (indexFromToken.containsKey(token)) return;
        if (bytes != null) indexFromToken.put(token, read(bytes));
    }

    private TreeMap<Long, ArrayList<Long>> read(byte[] bytes) {
        TreeMap<Long, ArrayList<Long>> map = new TreeMap<Long, ArrayList<Long>>();

        // If the word isn't in our index.
        if (bytes != null) {
            int[] postingList = Compressor.decodeVBytes(bytes);
            long offset = 0;
            int postingListIndex = 0;
            while (postingListIndex < postingList.length) {
                ArrayList<Long> list = new ArrayList<Long>();
                long relativeDocId = (long) postingList[postingListIndex++];
                relativeDocId += offset;
                offset = relativeDocId;
                map.put(relativeDocId, list);
                int numOccurrences = postingList[postingListIndex++];

                long occurrenceOffset = 0;
                for (int j = 0; j < numOccurrences; j++) {
                    long relativeOcc = postingList[postingListIndex++];
                    relativeOcc += occurrenceOffset;
                    occurrenceOffset = relativeOcc;
                    list.add(relativeOcc);
                }
            }
        }

        return map;
    }

    private TreeMap<Long, ArrayList<Long>> getDocumentsWithPhrase(String queryPhrase) {
        if (cachedPhrases.containsKey(queryPhrase))
            return cachedPhrases.get(queryPhrase);
        TreeMap<Long, ArrayList<Long>> result = new TreeMap<Long, ArrayList<Long>>();

        String[] tokens = queryPhrase.split("\\s+");
        Set<Long> docsWithAllTokens = getDocsWithTokens(Arrays.asList(tokens));

        for (Long docID : docsWithAllTokens) {
            Document doc = indexFromDocId.get(docID);
            long i = -1;
            // weHaveAWinner returns a positive index greater than i in the
            // document if the phrase was found. -1 otherwise.
            while ((i = weHaveAWinner(doc, tokens, i)) != -1){
                long id = doc._docid;
                if (!result.containsKey(id)) result.put(id, new ArrayList<Long>());
                result.get(id).add(i);
            }
        }
        cachedPhrases.put(queryPhrase, result);
        return result;
    }

    private long weHaveAWinner(Document doc, String[] tokens, long i) {
        Map<String, ArrayList<Long>> mapFromTokenToOccurrences = new HashMap<String, ArrayList<Long>>();
        for (String token : tokens) {
            getDocsWithToken(token);
            if (indexFromToken.containsKey(token)) mapFromTokenToOccurrences.put(token, indexFromToken.get(token).get((long) doc._docid));
        }
        if (mapFromTokenToOccurrences.get(tokens[0]) != null) {
            for (long occurrence : mapFromTokenToOccurrences.get(tokens[0])) {
                if (occurrence > i) {
                    boolean flag = true;
                    for (int j = 1; flag && j < tokens.length; j++) {
                        if (!mapFromTokenToOccurrences.containsKey(tokens[j]) || !mapFromTokenToOccurrences.get(tokens[j]).contains(occurrence + j)) flag = false;
                    }
                    if (flag) {
                        return occurrence;
                    }
                }
            }
        }
        return -1;
    }
}
