package net.hoodedice;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;

public class AddressJsonOutputFormat extends JsonOutputFormat<Text, ObjectWritable> {
    @Override
    protected String convertKey(Text key) {
        return null;
    }

    @Override
    protected JsonNode convertValue(ObjectWritable value) {
        return null;
    }
}
