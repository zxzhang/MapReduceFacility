package compute.mapper;

public interface Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
  void map(KEYIN key, VALUEIN value, Context context) ;
}
