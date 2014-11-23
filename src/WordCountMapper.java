
public class WordCountMapper implements Mapper{

    @Override
    public void map(String value, Output output) {
	for (String str : value.split("\\s+"))
	    output.write(str, "1");
    }

}
