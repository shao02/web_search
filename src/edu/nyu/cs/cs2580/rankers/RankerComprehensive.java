package edu.nyu.cs.cs2580.rankers;

import java.io.IOException;
import java.util.*;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;
import edu.nyu.cs.cs2580.helper.SpellingException;
import edu.nyu.cs.cs2580.index.Indexer;
import edu.nyu.cs.cs2580.models.Document;
import edu.nyu.cs.cs2580.models.QueryPhrase;
import edu.nyu.cs.cs2580.models.ScoredDocument;

/**
 * @author chenprice
 * @author
 * This ranker combines term features and
 * document-level features such as PageRank.
 */
public class RankerComprehensive extends Ranker {
    private static double PAGERANK_WEIGHT = 0.3;
    private static double TOKENS_WEIGHT = 0.7;
    private static final int QUERIES_TO_CACHE = 10;
    private static final double GOODNESS_RATIO = 1.5;
    private static final double STOP_WORD_RATIO = 0.5;
    Map<String, Double> tokenFrequencies;
    static LinkedHashMap<String, Vector<ScoredDocument>> cachedQueries = new LinkedHashMap<String, Vector<ScoredDocument>>(QUERIES_TO_CACHE, .5f, true) {
        @Override
        public boolean removeEldestEntry(Map.Entry eldest) {
            //when to remove the eldest entry
            return size() > QUERIES_TO_CACHE;   //size exceeded the max allowed
        }
    };

    public RankerComprehensive(Options options, CgiArguments arguments, Indexer indexer) {
        super(options, arguments, indexer);
        System.out.println("Using Ranker: " + this.getClass().getSimpleName());
        tokenFrequencies = new HashMap<String, Double>();
    }

    private boolean processWord(String token, StringBuilder spellingCorrection, QueryPhrase query) {
        double tokenFrequency = _indexer.corpusTermFrequency(token);
        tokenFrequencies.put(token, tokenFrequency);

        if (token.contains(" ")) {
            boolean spellingIssue = false;
            spellingCorrection.append("\"");
            StringBuilder quotedPart = new StringBuilder();
            for (String word : token.split("\\s")){
                double quotedTokenFrequency = _indexer.corpusTermFrequency(word);
                if (quotedTokenFrequency <= 1) {
                    quotedPart.append(correctSpelling(word, query)).append(" ");
                    spellingIssue = true;
                }
                else {
                    quotedPart.append(query.getRaw(word)).append(" ");
                }
            }
            spellingCorrection.append(quotedPart.toString().trim());
            spellingCorrection.append("\" ");
            return spellingIssue;
        }
        else {
            if (tokenFrequency <= 1) {
                spellingCorrection.append(correctSpelling(token, query)).append(" ");
                return true;
            } else {
                spellingCorrection.append(query.getRaw(token)).append(" ");
            }
        }
        return false;
    }

    private String correctSpelling(String token, QueryPhrase query) {
        try {
            int numSuggestions = 5;
            String[] suggestions = _indexer.getSpellChecker().suggestSimilar(query._tokens.contains(token) ? query.getRaw(token) : token, numSuggestions);
            if (suggestions != null && suggestions.length > 0) {
                Arrays.sort(suggestions, new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        return _indexer.corpusTermFrequency(o2) - _indexer.corpusTermFrequency(o1);
                    }
                });
                System.out.println("Sorted suggestions: " + Arrays.toString(suggestions));
                System.out.println("Did you mean " + suggestions[0] + "?");
                return suggestions[0];
            } else {
                return query.getRaw(token);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return token;
    }

    @Override
    public Vector<ScoredDocument> runQuery(QueryPhrase query, int numResults) throws SpellingException {
        System.out.println("Running " + query);
        if (cachedQueries.containsKey(query._query)) return cachedQueries.get(query._query);

        boolean spellingIssue = false;
        StringBuilder spellingCorrection = new StringBuilder();

        for (String token : query._tokens) {
            if (processWord(token, spellingCorrection, query)) spellingIssue = true;
        }
        if (spellingIssue) {
            throw new SpellingException(spellingCorrection.toString());
        }

        // stop words filtering
        int stop_th = (int) (STOP_WORD_RATIO * _indexer.numDocs());
        ArrayList<Integer> removalList = new ArrayList<Integer>();
        int j = 0;
        for (String token : query._tokens) {
            if (_indexer.corpusDocFrequencyByTerm(token) > stop_th) { // filter for stop words
                removalList.add(j);
                _indexer.removeFromCache(token);
            }
            j++;
        }
        int removed = 0;
        if (removalList.size() < query._tokens.size()) {
            for (Integer r : removalList) {
                System.out.println("stop_word removal -> [" + query._tokens.get(r - removed) + "]");
                query._tokens.remove(r - removed);
                removed++;
            }
        }

        System.out.println("Filtered query: " + query._tokens);
        Queue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>();

        try {
            long docId = 0;
            Document doc;
            int i = 0;
            while ((doc = _indexer.nextDoc(query, docId)) != null) {
                docId = doc._docid;
                ScoredDocument currDoc = score(i++, doc, query);
                queue.add(currDoc);
                if (queue.size() > numResults) queue.poll();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(":(");
        }
        Vector<ScoredDocument> result = new Vector<ScoredDocument>(queue);

        // normalize comprehensive score
        double pagerankMax = computeMaxPagerank(result);
        double tokensMax = computeMaxTokensScore(result);
        for (ScoredDocument sd : result) {
            integrateScores(sd, tokensMax, pagerankMax);
        }
        Collections.sort(result);
        Collections.reverse(result);

        cachedQueries.put(query._query, result);
        return result;
    }


    private ScoredDocument score(int i, Document doc, QueryPhrase query) {
        double resultByTokens = 1.0;
        for (String token : query._tokens) {
            // Probability of this term in this doc is the number of times this term
            // appears in this doc / the number of times the term appears in the corpus.
            // Our index only returns docs with all terms. So, we don't have to worry about
            // the case when the score is 0, rather than 1.
            double numOccurrences = _indexer.documentTermFrequency(token, doc.getUrl());
            resultByTokens *= numOccurrences / tokenFrequencies.get(token);
        }

        return new ScoredDocument(doc, resultByTokens);
    }

    private double computeMaxTokensScore(Vector<ScoredDocument> res){
        double max = -1;
        for (ScoredDocument sd : res){
            if (sd.getScore() > max)
                max = sd.getScore();
        }
        return max;
    }

    private double computeMaxPagerank(Vector<ScoredDocument> res){
        double max = -1;
        for (ScoredDocument sd : res){
            if (sd.getDoc().getPageRank() > max)
                max = sd.getDoc().getPageRank();
        }
        return max;
    }

    private void integrateScores(ScoredDocument doc, double tokensMax,
                                 double pagerankMax){

        // get normalized pagerank for this doc
        double currPageRank = (double)doc.getDoc().getPageRank() / pagerankMax;

        // get normalized tokensMax for this doc
        double currTokens = (double)doc.getScore() / tokensMax;

        // compute final result
        double finalWeightedResult =  TOKENS_WEIGHT * currTokens + PAGERANK_WEIGHT * currPageRank;

        doc.setScore(finalWeightedResult);
    }

}
