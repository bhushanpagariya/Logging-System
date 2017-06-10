package handlers.xsdhandle;


import javax.xml.bind.*;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * Created by bhushan on 5/13/17.
 */
public class XMLLoader {

    public static Object unmarshalXml(String xml) {
        StringReader reader = new StringReader(xml);
        try {
            JAXBContext jc = JAXBContext.newInstance( "src.main.java.xsds" );
            Unmarshaller u = jc.createUnmarshaller();
            return u.unmarshal(reader);
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String marshalXml(Object obj) {
        try {
            JAXBContext jc = JAXBContext.newInstance( "src.main.java.xsds" );
            Marshaller marshaller = jc.createMarshaller();
            StringWriter stringWriter = new StringWriter();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            marshaller.marshal(obj, stringWriter);

            return stringWriter.toString();
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return null;
    }

}
