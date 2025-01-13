package org.udayaw.window;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.*;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Arrays;
import java.util.Collection;

public class ControlEventWindow extends WindowFn<ControlElement<? extends Object>, IntervalWindow> {

    private Duration size;
    public ControlEventWindow(Duration size){
        this.size = size;
    }

    public boolean isNonMerging() {
        return false;
    }

    @Override
    public void mergeWindows(WindowFn<ControlElement<? extends Object>, IntervalWindow>.MergeContext c) throws Exception {
        MergeOverlappingIntervalWindows.mergeWindows(c);
    }

    @Override
    public final Collection<IntervalWindow> assignWindows(AssignContext c) {
        return Arrays.asList(assignWindow(c.timestamp(), c.element(), c.window()));
    }

    @Override
    public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
        throw new UnsupportedOperationException("Not allowed in side inputs");
    }

    public IntervalWindow assignWindow(Instant timestamp, ControlElement<?> element, BoundedWindow w) {

        if(element.isStartNewWindow()){
            return new IntervalWindow(timestamp, timestamp.plus(size.getMillis()));
        }
        return new IntervalWindow(timestamp, timestamp.plus(1));
    }

    @Override
    public final boolean assignsToOneWindow() {
        return true;
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
        return this.equals(other);
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
        return IntervalWindow.getCoder();
    }

}
