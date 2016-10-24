package testing;

import org.apache.commons.io.FileUtils;

import java.io.File;

public class TestArea {
    public static void main(String[] args) throws Exception {
        File file1 = new File("C:/Users/ocraw/Desktop/cypher_results.txt");
        File file2 = new File("C:/Users/ocraw/Desktop/pg_results.txt");
        System.out.println(FileUtils.contentEquals(file1, file2));
    }
}
