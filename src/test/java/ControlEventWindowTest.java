import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.udayaw.window.ControlElement;
import org.udayaw.window.ControlElementCoder;
import org.udayaw.window.ControlEventWindow;

import java.io.Serializable;

public class ControlEventWindowTest implements Serializable {

    public static final Logger LOGGER = LoggerFactory.getLogger(ControlEventWindowTest.class);

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    static {
        LogManager.getRootLogger().setLevel(Level.INFO);
    }





    @Test
    public void testControlEventWindow(){

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

        pipeline.apply(getDefaultTestStream())
                .apply(Window.into(new ControlEventWindow(Duration.standardHours(4))))
                .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers())).via(
                                (SerializableFunction<ControlElement<KV<String, Integer>>, KV<String, Integer>>) input -> input.getElement()
                        ))
                .apply(GroupByKey.create())

                .apply(ParDo.of(loggerFuncGBK));



        pipeline.run();

    }

    private TestStream<ControlElement<KV<String, Integer>>> getDefaultTestStream(){
        Instant startTime = Instant.parse("2024-01-01T00:00:00Z");
        String KEY = "key1";

        TestStream.Builder<ControlElement<KV<String, Integer>>> builder = TestStream.create(new ControlElementCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));

        for(int i = 0; i < 5; i++){
            if (i > 0)
                builder = builder.advanceProcessingTime(Duration.standardHours(i))
                        .advanceWatermarkTo(startTime.plus(Duration.standardHours(i)));

            builder = builder.addElements(TimestampedValue.of(new ControlElement<>(KV.of(KEY, i), i == 2), startTime.plus(Duration.standardHours(i))));
        }

        //later firing
        builder = builder.advanceProcessingTime(Duration.standardHours(24));
        builder = builder.advanceWatermarkTo(startTime.plus(Duration.standardHours(24)));
        builder = builder.addElements(TimestampedValue.of(new ControlElement<>(KV.of(KEY, 5)), startTime.plus(Duration.standardHours(23))));


        return builder.advanceWatermarkToInfinity();
    }



}
