package io.pravega;

import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.stream.StreamConfiguration;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import javax.swing.*;
import lombok.Cleanup;

public class ImageProcessing {

    final static String scopeName = "scope1";
    final static String streamName = "imageStream";
    final static String CONTROLLER_IP = "tcp://127.0.0.1:9090";

    public static void main(String[] args) throws Exception {

        // Configure the URL of the Pravega controller running
        // Pravega deployment using https://github.com/pravega/pravega/tree/master/docker/compose
        URI controllerURI = URI.create(CONTROLLER_IP);

        // Create scope and stream using the StreamManager APIs
        @Cleanup
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scopeName);
        streamManager.createStream(scopeName, streamName, StreamConfiguration.builder().build());

        // Create a ByteStreamClientFactory.
        @Cleanup
        ByteStreamClientFactory bf = ByteStreamClientFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerURI).build());

        // Write images to the Pravega Stream.
        @Cleanup
        ByteStreamWriter byteWriter = bf.createByteStreamWriter(streamName);
        // write image 1
        BufferedImage image = javax.imageio.ImageIO.read(new File(getFilePath("anatomy_of_log.jpg")));
        javax.imageio.ImageIO.write(image, "jpg", byteWriter);
        //write image 2
        BufferedImage image1 = javax.imageio.ImageIO.read(new File(getFilePath("deployment.arch.new.jpg")));
        javax.imageio.ImageIO.write(image1, "jpg", byteWriter);
        byteWriter.flush();
        streamManager.sealStream(scopeName, streamName);

        // Read images from the Pravega Stream using a ByteStreamReader
        @Cleanup
        ByteStreamReader byteStreamReader = bf.createByteStreamReader(streamName);
        List<BufferedImage> images = readAllImages(byteStreamReader);
        System.out.println("Completed reading images from the byte stream. Number of images read is " + images.size());
        displayImages(images);
    }

    private static String getFilePath(String imageName) {
        return ImageProcessing.class.getClassLoader().getResource(imageName).getFile();
    }

    private static List<BufferedImage> readAllImages(InputStream inputStream)
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
                    System.out.println("Read " + image);
                    images.add(image);
                }
            } catch (IndexOutOfBoundsException expected) {
                // We're done
            }

            imageReader.dispose();
        }
        return images;
    }

    private static void displayImages(List<BufferedImage> images) throws Exception {
        SwingUtilities.invokeAndWait(() ->
                                   {
                                       JFrame f = new JFrame();
                                       f.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
                                       f.getContentPane().setLayout(new GridLayout(1, 0));
                                       for (BufferedImage image : images) {
                                           f.getContentPane().add(new JLabel(new ImageIcon(image)));
                                       }
                                       f.pack();
                                       f.setLocationRelativeTo(null);
                                       f.setVisible(true);
                                   });
    }
}
