import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class LogProcessorSupplier implements ProcessorSupplier<byte[], byte[]> {

    private class LogProcessor implements Processor<byte[], byte[]> {

        private ProcessorContext context;

        @Override
        public void init(ProcessorContext processorContext) {
            this.context = processorContext;
        }

        @Override
        public void process(byte[] dummy, byte[] line) {
            // 把收集到的日志信息用string表示
            String input = new String(line);
            // 根据前缀MOVIE_RATING_PREFIX:从日志信息中提取评分数据
            if (input.contains("UserLog:")) {
                System.out.println("user log data coming!>>>>>>>>>>>" + input);

                input = input.split("UserLog:")[1].trim();
                context.forward("logProcessor".getBytes(), input.getBytes());
            }
        }

        @Override
        public void punctuate(long l) {

        }

        @Override
        public void close() {

        }
    }

    @Override
    public Processor<byte[], byte[]> get() {
        return new LogProcessor();
    }
}
