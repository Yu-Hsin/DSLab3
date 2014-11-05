import java.util.List;


public interface Reducer {
    public void reduce(String key, List <String> values, Output output);
}
