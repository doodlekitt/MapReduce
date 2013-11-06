public class DistFileSystem {

    private HashMap<Integer, List<String>> files =  new HashMap<Integer, List<String>>();

    int recordsperfile = 100;

    public ListFileLocations(String filename) {

    }

    public SplitFile(String filepath, int recordsize) throws FileNotFoundException {
        File file = new File(filepath);
    }
}
