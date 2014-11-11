package compute.mapper;

public interface Mapper<KEYOUT,VALUEOUT> {
  void map(String key, String value, Context context) ;
}
