package org.udayaw.window;

import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class ControlElementCoder<T extends Object> extends Coder<ControlElement<T>> {

    private Coder<T> elementCoder;
    private Coder<Boolean> booleanCoder;
    public ControlElementCoder(Coder<T> elementCoder){
        this.elementCoder = elementCoder;
        this.booleanCoder = BooleanCoder.of();
    }
    @Override
    public void encode(ControlElement<T> value,OutputStream outStream) throws CoderException, IOException {
        elementCoder.encode(value.getElement(), outStream);
        booleanCoder.encode(value.isStartNewWindow(), outStream);
    }

    @Override
    public ControlElement<T> decode(InputStream inStream) throws CoderException, IOException {
        return new ControlElement<>(elementCoder.decode(inStream), booleanCoder.decode(inStream));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return List.of(elementCoder, booleanCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        verifyDeterministic(this, "element coder must be deterministic!", elementCoder);
    }
}
