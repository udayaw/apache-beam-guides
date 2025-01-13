import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class Windowing implements Serializable {



    public static final Logger LOGGER = LoggerFactory.getLogger(Windowing.class);

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    static {
        LogManager.getRootLogger().setLevel(Level.INFO);
    }


    /**
     *
     *default test stream events would look like, NOT SAME KEY.
     * KV{1, 0} - 2024-01-01T00:00:00Z
     * KV{1, 1} - 2024-01-01T01:00:00Z
     * KV{1, 2} - 2024-01-01T02:00:00Z
     * KV{1, 3} - 2024-01-01T03:00:00Z
     * KV{1, 4} - 2024-01-01T04:00:00Z
     *
     * advance watermark to 2024-01-02 and send 1 hour late record.
     * KV{1, 4} - 2024-01-01T23:00:00Z
     */


    //Once triggers are not allowed anymore
    //https://s.apache.org/finishing-triggers-drop-data, must use with Repeatedly.forever()

    //see PaneInfo
    //https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/transforms/windowing/PaneInfo.Timing.html

    @Test
    public void testFixedWindows_AfterPane() {


        //windowing only become apparent at GBK operations

        //GBK will have atleast 4 windows.
        fireWithTrigger(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardDays(1)))
                .triggering(
                        Repeatedly.forever(
                                AfterPane.elementCountAtLeast(1)
                                        //.orFinally(AfterPane.elementCountAtLeast(2)) i believe, these are outdated after repeatedly has been introduced.
                        )
                )
                .withTimestampCombiner(TimestampCombiner.EARLIEST) //this denotes to what happen to each timestamp after GBK, default is END_OF_WINDOW
                .withAllowedLateness(Duration.standardHours(0)) //late value{5} will be skipped, unless we set withAllowedLateness(Duration.standardHours(1))
                .accumulatingFiredPanes());

    }

    @Test
    public void testFixedWindows_AfterProcessingTime() {

        fireWithTrigger(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardDays(1)))
                .triggering(
                        Repeatedly.forever(
                                //first sees element at 0 hour, and WAIT to fire until 2 -> collects 0,1,2 events
                                //then fire for each that arrives
                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardHours(2))
                        )
                )
                .withAllowedLateness(Duration.standardHours(0))
                .accumulatingFiredPanes());


    }

    @Test
    public void testFixedWindows_AfterWatermark() {


        fireWithTrigger(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardDays(1)))
                .triggering(
                        Repeatedly.forever(
                                AfterWatermark.pastEndOfWindow()
                        )
                )
                .withAllowedLateness(Duration.standardHours(0))
                .accumulatingFiredPanes());

    }


    @Test
    public void testSlidingWindow_AfterWatermark() {

        fireWithTrigger(Window.<KV<String, Integer>>into(SlidingWindows.of(Duration.standardHours(5))
                                .every(Duration.standardHours(1)))
                .triggering(
                        Repeatedly.forever(
                                AfterWatermark.pastEndOfWindow()
                        )
                )
                .withAllowedLateness(Duration.standardHours(0))
                .accumulatingFiredPanes()
        ,
                false //even without GBK we have duplicated elements in our PCollection becase slidingwindow is overlapping

        );


    }


    @Test
    public void testSessionWindow_AfterWatermark() {

        fireWithTrigger(
                Window.<KV<String, Integer>>into(Sessions.withGapDuration(Duration.standardHours(2)))
                        .triggering(
                                Repeatedly.forever(
                                        AfterWatermark.pastEndOfWindow()
                                )
                        )
                        .withAllowedLateness(Duration.standardHours(23))
                        .accumulatingFiredPanes()
                ,
                getSessionWindowTestStream()
                ,
                false

        );

    }

    @Test
    public void testCustomWindows_AfterWatermark() {


        fireWithTrigger(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardDays(1)))
                .triggering(
                        Repeatedly.forever(
                                AfterWatermark.pastEndOfWindow()
                        )
                )
                .withTimestampCombiner(TimestampCombiner.EARLIEST)
                .withAllowedLateness(Duration.standardHours(0))
                .accumulatingFiredPanes());

    }

    private TestStream<KV<String, Integer>> getSessionWindowTestStream(){
        //same as default test stream except delayed element has a timestamp at 0200 hours
        Instant startTime = Instant.parse("2024-01-01T00:00:00Z");
        String KEY = "key1";

        TestStream.Builder<KV<String, Integer>> builder = TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));

        for(int i = 0; i < 5; i++){
            if (i > 0)
                builder = builder.advanceProcessingTime(Duration.standardHours(i))
                        .advanceWatermarkTo(startTime.plus(Duration.standardHours(i)));

            builder = builder.addElements(TimestampedValue.of(KV.of(KEY, i), startTime.plus(Duration.standardHours(i)).plus(1000 * 60 * 5)));
        }

        //later firing
        builder = builder.advanceProcessingTime(Duration.standardHours(24));
        builder = builder.advanceWatermarkTo(startTime.plus(Duration.standardHours(24)));
        builder = builder.addElements(TimestampedValue.of(KV.of(KEY, 5), startTime.plus(Duration.standardHours(3))));


        return builder.advanceWatermarkToInfinity();
    }

    private TestStream<KV<String, Integer>> getDefaultTestStream(){
        Instant startTime = Instant.parse("2024-01-01T00:00:00Z");
        String KEY = "key1";

        TestStream.Builder<KV<String, Integer>> builder = TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));

        for(int i = 0; i < 5; i++){
            if (i > 0)
                builder = builder.advanceProcessingTime(Duration.standardHours(i))
                        .advanceWatermarkTo(startTime.plus(Duration.standardHours(i)));

            builder = builder.addElements(TimestampedValue.of(KV.of(KEY, i), startTime.plus(Duration.standardHours(i))));
        }

        //later firing
        builder = builder.advanceProcessingTime(Duration.standardHours(24));
        builder = builder.advanceWatermarkTo(startTime.plus(Duration.standardHours(24)));
        builder = builder.addElements(TimestampedValue.of(KV.of(KEY, 5), startTime.plus(Duration.standardHours(23))));


       return builder.advanceWatermarkToInfinity();
    }


    private void fireWithTrigger(Window<KV<String, Integer>> window){
        fireWithTrigger(window, getDefaultTestStream(), true);
    }

    private void fireWithTrigger(Window<KV<String, Integer>> window, boolean withGBK){
        fireWithTrigger(window, getDefaultTestStream(), withGBK);
    }

    private void fireWithTrigger(Window<KV<String, Integer>> window, TestStream<KV<String, Integer>> data, boolean withGBK) {



        PCollection<KV<String, Integer>> windowed = pipeline.apply(
                data
                )
                .apply(window);

        DoFn<KV<String, Iterable<Integer>>, KV<String, Integer>> loggerFuncGBK = new DoFn<KV<String, Iterable<Integer>>, KV<String, Integer>>() {
            @ProcessElement
            public void processElement(ProcessContext c, PaneInfo pane, BoundedWindow window) {
                LOGGER.info("Window{} ,{},  {}, watermark={}", window, pane, c.element(), c.timestamp());
                c.element().getValue().forEach(v->{
                    c.output(KV.of(c.element().getKey(), v));
                });
            }
        };

        DoFn<KV<String, Integer>, KV<String, Integer>> loggerFunc = new DoFn<KV<String, Integer>, KV<String, Integer>>() {
            @ProcessElement
            public void processElement(ProcessContext c, PaneInfo pane, BoundedWindow window) {
                LOGGER.info("Window{} ,{},  {}, watermark={}", window, pane, c.element(), c.timestamp());
                c.output(c.element());
            }
        };
        if(withGBK){
            windowed.apply(GroupByKey.create())
                    .apply(ParDo.of(loggerFuncGBK));
        }else{
            windowed.apply(ParDo.of(loggerFunc));
//                    .apply("w2", Window.into(new GlobalWindows())).apply("2", ParDo.of(loggerFunc));
        }

        pipeline.run();
    }

}
