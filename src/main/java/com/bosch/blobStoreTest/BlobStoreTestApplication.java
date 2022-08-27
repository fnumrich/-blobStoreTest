package com.bosch.blobStoreTest;

import ch.qos.logback.classic.Logger;
import com.azure.core.http.HttpClient;
import com.azure.core.http.okhttp.OkHttpAsyncHttpClientBuilder;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import lombok.SneakyThrows;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;

import static ch.qos.logback.classic.Level.WARN;
import static java.lang.String.format;

public class BlobStoreTestApplication {

    public static void main(String[] args) {
        // Prevent azure logs for more clear output
        Logger log = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        log.setLevel(WARN);

        int fileCount = 1000;
        int threads = 12;

        System.out.println(format("Threads, Average Push Duration (ms), Average Init & Push Duration (ms), Complete Duration (ms)"));
        for (int i = 1; i <= threads; i++) {
            run(i, fileCount);
        }
    }

    @SneakyThrows
    public static void run(int threads, int fileCount) {
        // Create a file (content) to upload several times
        byte[] file = createFile();

        // Credentials && Connection string
        String accountKey = "<accountKey>";
        String accountName = "<accountName>";
        String protocol = "https";
        String endpointSuffix = "core.windows.net";
        String connectStr = format("DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;EndpointSuffix=%s", protocol, accountName, accountKey, endpointSuffix);

        // Proxy settings to overcome the company proxy
        Properties props = System.getProperties();
        props.setProperty("java.net.useSystemProxies", "true");

        // The storage container to use
        String containerName = "quickstartblobs";

        // Azure SDK objects
        HttpClient httpClient = new OkHttpAsyncHttpClientBuilder().build();
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().httpClient(httpClient).connectionString(connectStr).buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);

        List<String> names = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            names.add("quickstart" + UUID.randomUUID() + ".txt");
        }

        // Helper set to track when each thread has done its first blob upload
        Set<Thread> threadsInitialized = new HashSet<>();
        // List for the first blob upload duration for each thread
        List<Long> initDurations = Collections.synchronizedList(new ArrayList<>());
        // List for the second and folowing blob upload duration of all threads
        List<Long> durations = Collections.synchronizedList(new ArrayList<>());

        ForkJoinPool forkJoinPool = new ForkJoinPool(threads);
        long completeStart = System.currentTimeMillis();
        forkJoinPool.submit(() ->
            names.parallelStream().forEach(name -> {
                long start = System.currentTimeMillis();
                upload(name, file, containerClient);
                long duration = System.currentTimeMillis() - start;
                //System.out.println(format("Duration: %sms", duration));
                if(!threadsInitialized.contains(Thread.currentThread())) {
                    threadsInitialized.add(Thread.currentThread());
                    initDurations.add(duration);
                } else {
                    durations.add(duration);
                }
            })
        ).get(); // Wait for all messages being processed
        long completeDuration = System.currentTimeMillis() - completeStart;

        // Average duration without init runs
        AtomicLong atomicLong = new AtomicLong();
        durations.stream().forEach(duration -> atomicLong.addAndGet(duration));
        long average = atomicLong.get() / durations.size();

        // Average init run duration
        AtomicLong initAtomicLong = new AtomicLong();
        initDurations.stream().forEach(initDuration -> initAtomicLong.addAndGet(initDuration));
        long initAverage = initAtomicLong.get() / initDurations.size();

//        System.out.println("****************************************************");
//        System.out.println(String.format("Thread count: %s", threads));
//        System.out.println(String.format("File count: %s", fileCount));
//        System.out.println(String.format("Complete duration: %sms", completeDuration));
//        System.out.println(String.format("Average init run duration: %sms", initAverage));
//        System.out.println(String.format("Average duration w/o initializing runs: %sms", average));
        System.out.println(format("%s, %s, %s, %s", threads, average, initAverage, completeDuration));
    }

    @SneakyThrows
    private static byte[] createFile() {
        byte[] file = new byte[50000];
        new Random().nextBytes(file);
        return file;
    }

    private static void upload(String name, byte[] bytes, BlobContainerClient containerClient) {
        BlobClient blobClient = containerClient.getBlobClient(name);
        blobClient.upload(BinaryData.fromBytes(bytes));
    }

}