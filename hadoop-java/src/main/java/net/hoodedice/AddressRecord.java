package net.hoodedice;

import java.util.ArrayList;
import java.util.List;

public class AddressRecord {
    public static class FieldDescription {
        public long id;
        public long zipcode;
        public String number;
        public String street;
        public String street2;
        public String city;
        public String state;
        public double latitude;
        public double longitude;
    }

    public static class RecordSchema {
        public RecordSchema(String message) {
            fields = new ArrayList<FieldDescription>();
            String[] elements = message.split("\n");
            for (String element : elements) {
                String line = element.trim().replace(";", "");
//                System.err.println("RecordSchema read line: " + line);
                if (line.startsWith("optional") || line.startsWith("required")) {
                    String[] parts = line.split(" ");
                    FieldDescription field = new FieldDescription();
                    field.id = Long.parseLong(parts[0]);
                    field.zipcode = Long.parseLong(parts[1]);
                    field.number = parts[2];
                    field.street = parts[3];
                    field.street2 = parts[4];
                    field.city = parts[5];
                    field.state = parts[6];
                    field.latitude = Double.parseDouble(parts[7]);
                    field.longitude = Double.parseDouble(parts[8]);

                    fields.add(field);
                }
            }
        }
        private List<FieldDescription> fields;
        public List<FieldDescription> getFields() {
            return fields;
        }
    }



}
