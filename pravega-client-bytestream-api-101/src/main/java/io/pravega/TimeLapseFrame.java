package io.pravega;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.image.BufferedImage;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A Custom JFrame to render a list of images as a Timelapse video.
 */
public class TimeLapseFrame extends JFrame {

    private static final int DELAY_MILLIS = 500;
    private final JLabel slidesLabel = new JLabel();
    private final List<ImageIcon> icons;
    private int currentSlide = -1;
    private final Timer timer;

    public TimeLapseFrame(final java.util.List<BufferedImage> images) {
        icons = images.stream().map(ImageIcon::new).collect(Collectors.toList());
        slidesLabel.setVerticalAlignment(JLabel.CENTER);
        slidesLabel.setHorizontalAlignment(JLabel.CENTER);
        setLayout(new BorderLayout());
        add(slidesLabel, BorderLayout.CENTER);
        nextSlide();
        timer = new Timer(DELAY_MILLIS, new TimerListener());
        timer.start();
    }

    private void nextSlide() {
        if (currentSlide < icons.size()) {
            currentSlide++;
            slidesLabel.setIcon(icons.get(currentSlide % icons.size()));
        }
    }

    private class TimerListener implements ActionListener
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
           nextSlide();
        }
    }
}
