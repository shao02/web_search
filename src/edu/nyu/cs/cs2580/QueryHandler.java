package edu.nyu.cs.cs2580;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import edu.nyu.cs.cs2580.SearchEngine.Options;
import edu.nyu.cs.cs2580.helper.SpellingException;
import edu.nyu.cs.cs2580.index.Indexer;
import edu.nyu.cs.cs2580.models.QueryPhrase;
import edu.nyu.cs.cs2580.models.ScoredDocument;
import edu.nyu.cs.cs2580.rankers.Ranker;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Vector;

/**
 * Handles each incoming query, students do not need to change this class except
 * to provide more query time CGI arguments and the HTML output.
 *
 * N.B. This class is not thread-safe. 
 *
 * @author congyu
 * @author fdiaz
 * 
 * 
 * 12-1-2014, Removed code for hw03 QueryRepresentations.
 * 
 */
public class QueryHandler implements HttpHandler {

    /**
     * CGI arguments provided by the user through the URL. This will determine
     * which Ranker to use and what output format to adopt. For simplicity, all
     * arguments are publicly accessible.
     */
    public static class CgiArguments {
        // The raw user query
        public String _query = "";
        // How many results to return
        private int _numResults = 10;

        // The type of the ranker we will be using.
        public enum RankerType {
            NONE,
            FULLSCAN,
            CONJUNCTIVE,
            FAVORITE,
            COSINE,
            PHRASE,
            QL,
            LINEAR,
            COMPREHENSIVE
        }
        public RankerType _rankerType = RankerType.NONE;

        // The output format.
        public enum OutputFormat {
            TEXT,
            HTML,
            RESULTS
        }
        public OutputFormat _outputFormat = OutputFormat.TEXT;

        public int _numdocs;
        public int _numterms;

        public CgiArguments(String uriQuery) {
            String[] params = uriQuery.split("&");
            for (String param : params) {
                String[] keyval = param.split("=", 2);
                if (keyval.length < 2) {
                    continue;
                }
                String key = keyval[0].toLowerCase();
                String val = keyval[1];
                if (key.equals("query")) {
                    _query = val;
                } else if (key.equals("num")) {
                    try {
                        _numResults = Integer.parseInt(val);
                    } catch (NumberFormatException e) {
                        // Ignored, search engine should never fail upon invalid user input.
                    }
                } else if (key.equals("ranker")) {
                    try {
                        _rankerType = RankerType.valueOf(val.toUpperCase());
                    } catch (IllegalArgumentException e) {
                        // Ignored, search engine should never fail upon invalid user input.
                    }
                } else if (key.equals("format")) {
                    try {
                        _outputFormat = OutputFormat.valueOf(val.toUpperCase());
                    } catch (IllegalArgumentException e) {
                        // Ignored, search engine should never fail upon invalid user input.
                    }
                }else if (key.equals("numdocs")) {
                    try {
                        _numdocs = Integer.parseInt(val);
                    } catch (IllegalArgumentException e) {
                        // Ignored, search engine should never fail upon invalid user input.
                    }
                }else if (key.equals("numterms")) {
                    try {
                        _numterms = Integer.parseInt(val);
                    } catch (IllegalArgumentException e) {
                        // Ignored, search engine should never fail upon invalid user input.
                    }
                }
            }  // End of iterating over params
        }
    }

    // For accessing the underlying documents to be used by the Ranker. Since
    // we are not worried about thread-safety here, the Indexer class must take
    // care of thread-safety.
    private Indexer _indexer;

    public QueryHandler(Options options, Indexer indexer) {
        _indexer = indexer;
    }

    private void respondWithMsg(HttpExchange exchange, final String message, String contentType)
            throws IOException {
        Headers responseHeaders = exchange.getResponseHeaders();
        responseHeaders.set("Content-Type", contentType);
        exchange.sendResponseHeaders(200, 0); // arbitrary number of bytes
        OutputStream responseBody = exchange.getResponseBody();
        responseBody.write(message.getBytes());
        responseBody.close();
    }

    private void constructTextOutput(final Vector<ScoredDocument> docs, StringBuffer response) {
        for (ScoredDocument doc : docs) {
            response.append(response.length() > 0 ? "\n" : "");
            response.append(doc.toString());
        }
        response.append(response.length() > 0 ? "\n" : "");
    }

    private void constructTextOutput(String message, StringBuffer response) {
        response.append("Did you mean '").append(message).append("'?\n");
        response.append("No results found.");
    }

    private void constructResultOnlyOutput(final Vector<ScoredDocument> docs, StringBuffer response, double time, String query) {
        String tableHeader = readFile("html/result_table.header.partial.html");
        tableHeader = fill(tableHeader, "query", query);
        tableHeader = fill(tableHeader, "time", Double.toString(time));
        response.append(tableHeader);
        for (ScoredDocument doc : docs) {
            response.append(buildDocHTML(doc)).append('\n');
        }
        response.append(readFile("html/result_table.footer.partial.html"));
    }

    private void constructHTMLOutput(final Vector<ScoredDocument> docs, StringBuffer response, double time, String query) {
        response.append(fill(readFile("html/header.partial.html"), "value", query));
        String tableHeader = readFile("html/result_table.header.partial.html");
        tableHeader = fill(tableHeader, "query", query);
        tableHeader = fill(tableHeader, "time", Double.toString(time));
        response.append(tableHeader);
        for (ScoredDocument doc : docs) {
            response.append(buildDocHTML(doc)).append('\n');
        }
        response.append(readFile("html/result_table.footer.partial.html"));
        response.append(readFile("html/footer.partial.html"));
    }

    private void constructHTMLOutput(String message, StringBuffer response, String query) {
        try {
            response.append(fill(readFile("html/header.partial.html"), "value", query));
            response.append("Did you mean <a href=\"/search?query=")
                    .append(URLEncoder.encode(message, "UTF-8").trim())
                    .append("&format=html\">").append(message.trim())
                    .append("</a>?\n");
            response.append("<p> No results found. </p>\n");
            response.append(readFile("html/footer.partial.html"));
        } catch (UnsupportedEncodingException e) {
            // Should never happen.
        }
    }

    private String buildDocHTML(ScoredDocument doc) {
        String partial = readFile("html/doc.partial.html");
        partial = partial.replaceAll("\\{\\{docid\\}\\}", Long.toString(doc.getDocumentId()));
        partial = partial.replaceAll("\\{\\{title\\}\\}", doc.getDoc().getTitle());
        partial = partial.replaceAll("\\{\\{pagerank\\}\\}", Float.toString(doc.getDoc().getPageRank()));
        partial = partial.replaceAll("\\{\\{score\\}\\}", Double.toString(doc.getScore()));
        partial = partial.replaceAll("\\{\\{url\\}\\}", doc.getDoc().getUrl());
        return partial;
    }

    private String fill(String content, String pattern, String replacement) {
        return content.replaceAll("\\{\\{" + pattern + "\\}\\}", replacement);
    }

    public void handle(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        if (!requestMethod.equalsIgnoreCase("GET")) { // GET requests only.
            return;
        }

        // Print the user request header.
        Headers requestHeaders = exchange.getRequestHeaders();
        /*System.out.print("Incoming request: ");
        for (String key : requestHeaders.keySet()) {
            System.out.print(key + ":" + requestHeaders.get(key) + "; ");
        }
        System.out.println();*/

        // Validate the incoming request.
        String uriQuery = exchange.getRequestURI().getQuery();
        String uriPath = exchange.getRequestURI().getPath();
        if (uriPath == null) {
            respondWithMsg(exchange, "Something wrong with the URI path!", "text/plain");
            return;
        }
        else if (uriPath.contains("favicon.ico")) {
            respondWithMsg(exchange, "", "text/html");  // Dummy response to avoid timeout bug.
            return;
        }
        else if (uriPath.endsWith("css")) {
            respondWithMsg(exchange, readFile(uriPath), "text/css");
            return;
        }
        else if (uriPath.equals("/")) {
            StringBuilder sb = new StringBuilder();
            sb.append(fill(readFile("html/header.partial.html"), "value", ""));
            sb.append(readFile("html/footer.partial.html"));
            respondWithMsg(exchange, sb.toString(), "text/html");
            return;
        }
        else if (!uriPath.equals("/search") && !uriPath.equals("/prf")) {
            respondWithMsg(exchange, readFile(uriPath), "text/html");
            return;
        }
        else if (uriQuery == null) {
            respondWithMsg(exchange, "Something wrong with the query!", "text/plain");
            return;
        }
        System.out.println("Query: " + uriQuery);

        // Process the CGI arguments.
        CgiArguments cgiArgs = new CgiArguments(uriQuery);
        if (cgiArgs._query.isEmpty()) {
            respondWithMsg(exchange, "No query is given!", "text/plain");
        }

        // Create the ranker.
        Ranker ranker = Ranker.Factory.getRankerByArguments(
                cgiArgs, SearchEngine.OPTIONS, _indexer);
        if (ranker == null) {
            respondWithMsg(exchange,
                    "Ranker " + cgiArgs._rankerType.toString() + " is not valid!", "text/plain");
        }

        // Processing the query.
        QueryPhrase processedQuery = new QueryPhrase(cgiArgs._query);
        processedQuery.processQuery();

        System.out.println("Running the query: " + processedQuery._tokens);
        // Ranking.
        double time = System.currentTimeMillis();
        Vector<ScoredDocument> scoredDocs;
        StringBuffer response = new StringBuffer();
        String contentType = "text/plain";
        try {
            scoredDocs = ranker.runQuery(processedQuery, cgiArgs._numResults);
            time = (System.currentTimeMillis() - time) / 1000.0;
            System.out.println("Found " + scoredDocs.size() + " / " + cgiArgs._numResults + " docs.");
            switch (cgiArgs._outputFormat) {
                case TEXT:
                    constructTextOutput(scoredDocs, response);
                    break;
                case HTML:
                    constructHTMLOutput(scoredDocs, response, time, processedQuery.toString());
                    contentType = "text/html";
                    break;
                case RESULTS:
                    constructResultOnlyOutput(scoredDocs, response, time, processedQuery.toString());
                default:
                    // nothing
            }
        } catch (SpellingException e) {
            switch (cgiArgs._outputFormat) {
                case TEXT:
                    constructTextOutput(e.getMessage(), response);
                    break;
                case HTML:
                    constructHTMLOutput(e.getMessage(), response, processedQuery.toString());
                    contentType = "text/html";
                    break;
                default:
                    // nothing
            }
            try {
                processedQuery = new QueryPhrase(e.getMessage());
                processedQuery.processQuery();
                ranker.runQuery(processedQuery, cgiArgs._numResults);
            } catch (SpellingException e2) {
                // Just give up on this, for now.
            }
        }
        respondWithMsg(exchange, response.toString(), contentType);
        System.out.println("Finished query: [" + cgiArgs._query + "] in " + time + " seconds.");
    }

    private String readFile(String filePath) {
        String pwd = System.getenv("PWD");
        if (filePath.charAt(0) == '/' && !filePath.matches(".*" + pwd + ".*")) filePath = pwd + filePath;
        StringBuilder result = new StringBuilder();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line).append('\n');
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result.toString();
    }
}

