import java.util.List;


public class RemoveNoiseReducer implements Reducer{
    public String filter = "a";
    @Override
    public void reduce(String key, List<String> values, Output output) {
	if (key.contains(filter))
	    return;
	else
	    output.write(key, "");
    }

}
