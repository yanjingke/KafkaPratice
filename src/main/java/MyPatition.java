import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

public class MyPatition implements Partitioner {
    private static Logger logger = Logger.getLogger(MyPatition.class);
    public MyPatition(VerifiableProperties props) {

    }


    public int partition(Object o, int i) {
        return Integer.parseInt(o.toString())%i;
    }
}
