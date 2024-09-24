import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GenreMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final static LongWritable one = new LongWritable(1);
    private Text genre = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Assuming the CSV format: title,genre,... (adjust as necessary)
        String[] fields = value.toString().split(",");
        if (fields.length > 1) {
            genre.set(fields[1].trim()); // Assuming genre is the second column
            context.write(genre, one);
        }
    }
}
