import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Random;

public class CacheIPCTest {

    private static String output(InputStream inputStream) throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line + System.getProperty("line.separator"));
            }
        } finally {
            br.close();
        }
        return sb.toString();
    }


    public static void startAnotherJVM() throws Exception {
        String separator = System.getProperty("file.separator");
        String classpath = System.getProperty("java.class.path");
        String path = System.getProperty("java.home")
                + separator + "bin" + separator + "java";
        ProcessBuilder processBuilder =
                new ProcessBuilder(path, "-cp",
                        classpath,
                        CacheTest.class.getName());
        Process process = processBuilder.start();
        System.out.println("Output:\n" + output(process.getInputStream()));
        process.waitFor();
    }

    public static void startAndKillAnotherJVM() throws Exception {
        String separator = System.getProperty("file.separator");
        String classpath = System.getProperty("java.class.path");
        String path = System.getProperty("java.home")
                + separator + "bin" + separator + "java";
        ProcessBuilder processBuilder =
                new ProcessBuilder(path, "-cp",
                        classpath,
                        CacheTest.class.getName());
        Process process = processBuilder.start();
        Random rnd = new Random(new Date().getTime() / 1000);
        Thread.sleep(rnd.nextInt() % 1000, rnd.nextInt());
        process.destroy();
        System.out.println("Distroyed Output:\n" + output(process.getInputStream()));
        process.waitFor();
    }


    public static void main(String[] args) throws Exception {
        int numOfJVMs = 5;

        Runnable r1 = new Runnable() {

            public void run() {
                try {
                    startAnotherJVM();
                } catch (Exception e) {
                    throw (new Error("JVM is flying ...."));
                }
            }
        };


        Runnable r2 = new Runnable() {

            public void run() {
                try {
                    startAndKillAnotherJVM();
                } catch (Exception e) {
                    throw (new Error("JVM is flying ...."));
                }
            }
        };


        Thread[] tArray = new Thread[numOfJVMs];

        Runnable r = r1;

        for (int i = 0; i < numOfJVMs; i++) {
            if (i == numOfJVMs - 2)
                r = r2;
            tArray[i] = new Thread(r);
            tArray[i].start();
        }

        for (int i = 0; i < numOfJVMs; i++) {
            tArray[i].join();
        }
    }
}
