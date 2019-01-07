
import org.apache.log4j.Logger;
public class LoggerGenerator {
    /**
     * 模拟日志产生
     *
     */
    private static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());

    public static void main(String[] args) throws Exception{

        int index = 0;
        while(true) {
            Thread.sleep(1000);
            logger.info("value : " + index++);
        }
    }

}
