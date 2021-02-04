package io.pravega;

import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.stream.StreamConfiguration;
import lombok.Cleanup;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import javax.swing.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TimeLapse {

    final static String scopeName = "scope2";
    final static String streamName = "ndvu";
    final static String CONTROLLER_IP = "tcp://127.0.0.1:9090";

    public static void main(String[] args) throws Exception {

        // Configure the URL of the Pravega controller running
        // Pravega deployment using https://github.com/pravega/pravega/tree/master/docker/compose
        URI controllerURI = URI.create(CONTROLLER_IP);

        // Create scope and stream using the StreamManager APIs
        @Cleanup
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scopeName);
        // In-memory lookup to track offsets.
        Map<LocalDate, Long> offsetMap = new HashMap<>(12);
        // Create a ByteStreamClientFactory.
        @Cleanup
        ByteStreamClientFactory bf = ByteStreamClientFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerURI).build());

        if (streamManager.createStream(scopeName, streamName, StreamConfiguration.builder().build())) {
            // Write all images to the Pravega Stream.
            @Cleanup
            ByteStreamWriter byteWriter = bf.createByteStreamWriter(streamName);
            for (int month = 1; month <= 12; month++) {
                BufferedImage image = null;
                // ByteStreamWriter#fetchTailOffset() can be be used to fetch the current offset in the ByteStream.
                // Store the offset at which image file is being stored.
                offsetMap.put(LocalDate.of(2010, month, 1), byteWriter.fetchTailOffset());
                image = ImageIO.read(new File(getFilePath("timelapse/ndvi_" + month + ".jpg")));
                ImageIO.write(image, "jpg", byteWriter);
            }
            byteWriter.flush();
        }
        streamManager.sealStream(scopeName, streamName);
        // Read images from the Pravega Stream using a ByteStreamReader
        @Cleanup
        ByteStreamReader byteStreamReader = bf.createByteStreamReader(streamName);

        // ByteStreamReader#seekToOffset enables the us to read events from a particular offset.
        // Read all images from February
        byteStreamReader.seekToOffset(offsetMap.get(LocalDate.of(2010, 2, 1)));
        List<BufferedImage> images = readAllImages(byteStreamReader);
        System.out.println("Completed reading images from the byte stream. Number of images read is " + images.size());
        displayAsSlideShow(images);
    }

    private static String getFilePath(String imageName) {
        return TimeLapse.class.getClassLoader().getResource(imageName).getFile();
    }

    private static List<BufferedImage> readAllImages(ByteStreamReader inputStream)
            throws IOException {
        //  ref: https://stackoverflow.com/a/53501316/3182664
        List<BufferedImage> images = new ArrayList<BufferedImage>();
        try (ImageInputStream in = javax.imageio.ImageIO.createImageInputStream(inputStream)) {
            //Get a list of all registered ImageReaders that claim to be able to decode the image (JPG, PNG...)
            Iterator<ImageReader> imageReaders = javax.imageio.ImageIO.getImageReaders(in);

            if (!imageReaders.hasNext()) {
                throw new AssertionError("No imageReader for the given format " + inputStream);
            }

            ImageReader imageReader = imageReaders.next();
            imageReader.setInput(in);

            // It's possible to use reader.getNumImages(true) and a for-loop
            // here.
            // However, for many formats, it is more efficient to just read
            // until there's no more images in the stream.
            try {
                int i = 0;
                while (true) {
                    BufferedImage image = imageReader.read(i++);
                    images.add(image);
                }
            } catch (IndexOutOfBoundsException expected) {
                // We're done
            }

            imageReader.dispose();
        }
        return images;
    }

    private static void displayAsSlideShow(List<BufferedImage> images) throws Exception {
        SwingUtilities.invokeAndWait(() ->
        {
            TimeLapseFrame frame = new TimeLapseFrame(images);
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.pack();
            frame.setLocationRelativeTo(null);
            frame.setVisible(true);
        });
    }
}
