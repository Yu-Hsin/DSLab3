import java.util.List;


public class TestReducer implements Reducer{

    @Override
    public void reduce(String key, List<String> values, Output output) {
	
	int sum = 0;
	for (String str : values) {
	    sum += Integer.parseInt(str);
	}
	output.write(key, String.valueOf(sum));
    }

}
