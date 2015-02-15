package edu.nyu.cs.cs2580.index;

import edu.nyu.cs.cs2580.SearchEngine;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Class to facilitate writing and merging temporary index files.
 * <p/>
 * Write a temporary index file with a call to write(TreeMap<String, String>),
 * where the key is a token and the value is the posting list for that token.
 * For example:
 * "hello" => 1 [2 [4 9]] 10 [1 [40]] 100 [2 [9 10]]
 * without the [] and "".
 * <p/>
 * You should write a temporary file every IndexOrganizer.DOCS_PER_GROUP documents.
 * <p/>
 * Once you're finished writing the temporary files (make sure to write all files!),
 * call
 * mergeDocumentIndices();
 * and
 * organizer.splitIndex();
 * <p/>
 * The first call merges all temporary files into a single huge posting list with no
 * repeats.
 * <p/>
 * The second call writes out final index files for each some number of words.
 * <p/>
 * When processing queries, call loadIndexFor(String token). This will return a
 * TreeMap<String, String> in the same format as the argument you gave for write.
 * <p/>
 * The caller is responsible for proper encoding of the String posting list. This class
 * does not discriminate.
 */
public class IndexOrganizer {

    private static final String FULL_INDEX = "/full_corpus.idx";
    public static final String DOC_INDEX_PREFIX = "document_index_";
    public static final String DICTIONARY_FILENAME = "dictionary";
    private static final String jsonPrefix = "js/lookup_";

    private static int bytesPerFinalIndex;
    private static int numMergingThreads;
    private static String finalIndexName;
    SearchEngine.Options _options;

    private Indexer indexer = null;

    protected static final int TOKENS_TO_CACHE = 10;

    LinkedHashMap<String, byte[]> cachedTokens = new LinkedHashMap<String, byte[]>(10, .5f, true) {
        @Override
        public boolean removeEldestEntry(Map.Entry eldest) {
            return size() > TOKENS_TO_CACHE;   //size exceeded the max allowed
        }
    };

    public IndexOrganizer(SearchEngine.Options options, Indexer indexer) {
        this._options = options;
        finalIndexName = _options._indexPrefix + FULL_INDEX;
        this.indexer = indexer;
        bytesPerFinalIndex = options._bytes_per_final_index;
        numMergingThreads = options._num_merging_threads;
    }

    public void write(TreeMap<String, List<Long>> postingList, String filename) {
        try {
            File newIndex = new File(_options._indexPrefix + "/" + filename);
            OutputStreamWriter writer = new FileWriter(newIndex);

            for (String token : postingList.keySet()) {
                writer.write(token);
                for (long i : postingList.get(token)) writer.write(" " + i);
                writer.write('\n');
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void mergeAndSplit() throws IOException {
        mergeDocumentIndices();
        splitIndex();
    }


    /**
     * Take all document group index files in the index directory and merge them
     * into a single large file.
     *
     * @throws java.io.IOException
     */
    private void mergeDocumentIndices() throws IOException {
        String finalIndexPath = _options._indexPrefix + FULL_INDEX;
        File indexDirectory = new File(_options._indexPrefix);

        List<File> indices = Arrays.asList(indexDirectory.listFiles());
        List<File> copy = new ArrayList<File>(indices);
        for (File f : indices) {
            if (!f.getName().contains(DOC_INDEX_PREFIX)) copy.remove(f);
        }
        indices = copy;

        Collections.sort(indices, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                int i1 = Integer.parseInt(
                        o1.getName().substring(
                                o1.getName().lastIndexOf(DOC_INDEX_PREFIX.charAt(DOC_INDEX_PREFIX.length() - 1)) + 1));
                int i2 = Integer.parseInt(
                        o2.getName().substring(
                                o2.getName().lastIndexOf(DOC_INDEX_PREFIX.charAt(DOC_INDEX_PREFIX.length() - 1)) + 1));
                return i1 - i2;
            }
        });
        File tempFinalIndex = mergeDocumentIndices(indices);
        File finalIndex = new File(finalIndexPath);
        tempFinalIndex.renameTo(finalIndex);
    }

    /**
     * Take a list of files which contain posting lists and merge them all into a single list.
     *
     * @param files
     * @return
     */
    private File mergeDocumentIndices(List<File> files) {
        if (files.size() == 1) return files.get(0);
        final List<File> firstHalf = new ArrayList<File>(files.subList(0, files.size() / 2));
        final List<File> secondHalf = new ArrayList<File>(files.subList(files.size() / 2, files.size()));
        ExecutorService executorService = Executors.newFixedThreadPool(numMergingThreads);
        List<Future<File>> threads = new ArrayList<Future<File>>();
        Callable<File> t1 = new Callable<File>() {
            @Override
            public File call() throws Exception {
                return mergeDocumentIndices(firstHalf);
            }
        };
        Callable<File> t2 = new Callable<File>() {
            @Override
            public File call() throws Exception {
                return mergeDocumentIndices(secondHalf);
            }
        };
        threads.add(executorService.submit(t1));
        threads.add(executorService.submit(t2));

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            File mergedFirstHalf = threads.get(0).get();
            File mergedSecondHalf = threads.get(1).get();
            return mergeDocumentIndices(mergedFirstHalf, mergedSecondHalf);
        } catch (InterruptedException e) {
            System.err.println("Threads timed out!!");
            e.printStackTrace();
        } catch (ExecutionException e) {
            System.err.println("Execution issue...");
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Take two files containing posting lists and merge them into one.
     * The two input files are deleted!
     *
     * @param index1
     * @param index2
     * @return the merged posting list.
     */
    private File mergeDocumentIndices(File index1, File index2) {
        System.out.println("Merging " + index1.getName() + " and " + index2.getName());
        String name = index1.getName() + "_" + index2.getName();
        name = name.replaceAll("merge_", "");
        name = name.replaceAll(DOC_INDEX_PREFIX, "");
        File mergedIndex = new File(index1.getParentFile(), "merge_" + name);
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(mergedIndex));
            BufferedReader reader1 = new BufferedReader(new FileReader(index1));
            BufferedReader reader2 = new BufferedReader(new FileReader(index2));
            String line1 = reader1.readLine(), line2 = reader2.readLine();
            while (line1 != null && line2 != null) {
                String token1 = line1.substring(0, line1.indexOf(" "));
                String token2 = line2.substring(0, line2.indexOf(" "));
                if (token1.compareTo(token2) > 0) {
                    writer.append(line2).append('\n');
                    line2 = reader2.readLine();
                } else if (token1.compareTo(token2) < 0) {
                    writer.append(line1).append('\n');
                    line1 = reader1.readLine();
                } else { // The two tokens are the same.
                    writer.append(line1).append(" ").append(line2.substring(token1.length()).trim()).append('\n');
                    line1 = reader1.readLine();
                    line2 = reader2.readLine();
                }
            }
            while (line1 != null) {
                writer.append(line1).append('\n');
                line1 = reader1.readLine();
            }
            while (line2 != null) {
                writer.append(line2).append('\n');
                line2 = reader2.readLine();
            }
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        index1.delete();
        index2.delete();
        return mergedIndex;
    }


    /**
     * Split the given full index into many indices with at most numBytesPerIndex
     * per index.
     */
    private void splitIndex() {
        File fullIndex = new File(finalIndexName);
        RandomAccessFile outputFile = null;


        char jsonFileIndex = ']';
        String jsonFileName;
        FileWriter jsonWriter = null;
        StringBuffer input = new StringBuffer();

        try {
            FileWriter dictionary = new FileWriter(DICTIONARY_FILENAME);
            BufferedReader reader = new BufferedReader(new FileReader(fullIndex));
            String line;
            int bytesWritten = 0;
            while ((line = reader.readLine()) != null) {

                String token = line.substring(0, line.indexOf(" ")).trim();
                if (token.length() > 20 || token.matches(".*[0-9].*")) continue;

                if (token.matches("[A-Za-z]+")) dictionary.write(token + "\n");

                if (token.length() > 1 && token.matches("[a-z]+")) {
                    char firstChar = Character.toLowerCase(token.charAt(0));
                    if (jsonFileIndex == firstChar) {
                        input.append("\"").append(token).append("\",");
                    } else {
                        if (jsonWriter != null) {
                            input.append("\"END\"]");
                            jsonWriter.write(input.toString());
                            jsonWriter.flush();
                            jsonWriter.close();
                        }
                        jsonFileIndex = firstChar;
                        jsonFileName = jsonPrefix + jsonFileIndex + "_.json";
                        jsonWriter = new FileWriter(jsonFileName);
                        input = new StringBuffer();
                        input.append("[");
                        token = "\"" + token + "\",";
                        input.append(token);
                    }
                }

                line = line.substring(line.indexOf(" ") + 1).trim();
                if (outputFile == null) {
                    String name = _options._indexPrefix + "/" + token + ".idx";
                    System.out.println("Writing " + name);
                    outputFile = new RandomAccessFile(name, "rw");
                }

                StringTokenizer st = new StringTokenizer(line);
                List<Long> postingListList = new ArrayList<Long>();
                while (st.hasMoreTokens()) {
                    postingListList.add(Long.parseLong(st.nextToken()));
                }
                long[] postingList = new long[postingListList.size()];
                int idx = 0;
                for (long p : postingListList) postingList[idx++] = p;

                byte[] lineBytes = indexer.encode(postingList);

                outputFile.writeUTF(token);
                outputFile.writeInt(lineBytes.length);
                outputFile.write(lineBytes);

                bytesWritten += lineBytes.length;

                if (bytesWritten >= bytesPerFinalIndex) {
                    outputFile.close();
                    outputFile = null;
                    bytesWritten = 0;
                }
            }
            if (jsonWriter != null) {
                input.append("\"END\"]");
                jsonWriter.write(input.toString());
                jsonWriter.flush();
                jsonWriter.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        fullIndex.delete();
    }


    public byte[] loadIndexFor(String token) {
        if (cachedTokens != null && cachedTokens.containsKey(token)) {
            return cachedTokens.get(token);
        }
        File[] indices = new File(_options._indexPrefix).listFiles();

        Arrays.sort(indices, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        File finalIndex = null;
        for (File f : indices) {
            String fileName = f.getName();
            if (!fileName.endsWith(".idx")) continue;
            if (fileName.substring(0, fileName.indexOf(".idx")).compareTo(token) > 0) {
                break;
            }
            finalIndex = f;
        }
        byte[] bytes = buildIndexFor(finalIndex, token);
        cachedTokens.put(token, bytes);
        return bytes;
    }

    public boolean removeFromCache(String token) {
        if (cachedTokens.containsKey(token)) {
            cachedTokens.remove(token);
            return true;
        } else return false;
    }

    private byte[] buildIndexFor(File index, String token) {
        try {
            RandomAccessFile indexFile = new RandomAccessFile(index, "r");
            try {
                while (true) {
                    String thisToken = indexFile.readUTF();
                    int numBytes = indexFile.readInt();
                    byte[] bytes = new byte[numBytes];
                    indexFile.read(bytes);
                    if (thisToken.equals(token)) return bytes;
                }
            } catch (EOFException e) {
                // End of file, no more tokens. Ignore.
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}