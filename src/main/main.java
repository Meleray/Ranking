import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Worker;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Click_Pattern implements Writable {
    public String query  =  "";
    public int geolocation  =  -1;
    public List <String> link_show  =  new ArrayList <>();
    public List <String> link_click  =  new ArrayList <>();
    public List <String> host_show  =  new ArrayList <>();
    public List <String> host_click  =  new ArrayList <>();
    public List <Boolean> pos_clicks  =  new ArrayList <Boolean>();
    public List <Long> timestamps  =  new ArrayList <>();
    public Map < String, Integer> time_map  =  new HashMap <>();
    public boolean timings;

    public Click_Pattern() {
    }

    public Click_Pattern(final String query, final int geolocation, final List < String> link_show, final List < String> link_click) {
        this.query  =  query;
        this.geolocation  =  geolocation;
        this.link_show  =  link_show;
        this.link_click  =  link_click;
    }

    public Click_Pattern(final String query, final int geolocation, final List < String> link_show, final List < String> link_click, final List < Long> timestamps) {
        this.query  =  query;
        this.geolocation  =  geolocation;
        this.link_show  =  link_show;
        this.link_click  =  link_click;
        this.timestamps  =  timestamps;
    }

    public Click_Pattern(final String data){
        this.parse_str(data);
    }


    public static String parse_query(final String query) {
        return query.trim();
    }

    public static String parse_host(final String url) throws URISyntaxException {
        String hurl  = url;
        int ind  =  hurl.toString().indexOf("/");
        if (ind =  = -1)
            return hurl;
        String h  =   hurl.substring(0,ind);
        return h;
    }

    public void parse_str(final String in) {
        int idx  =  0;
        final String[] args  =  in.split('\t');
        final String[] query_args  =  args[idx++].split('@');
        query  =  parse_query(query_args[0]);
        geolocation  =  Integer.parseInt(query_args[1]);
        link_show  =  new ArrayList < >();
        String tmp  =  args[idx++].replace(",https://",",http://");
        tmp  =  tmp.replaceFirst("^https://", "http://");
        tmp  =  tmp.replaceAll(",(?!http://)([a-zA-Z]+)", ",http://$1");
        tmp  =  tmp.replaceFirst("^http://", "");
        String[] link_show_args  =  tmp.split(",http://");
        for ( String url : link_show_args) {  
            url  =  url.startsWith("wwww.")?url.substring(4):url;  
            link_show.add(url);
            host_show.add(parse_host(url));
        }
        link_click  =  new ArrayList <>();
        tmp  =   args[idx++].replace(",https://",",http://");
        tmp  =  tmp.replaceFirst("^https://", "http://");
        tmp  =  tmp.replaceAll(",(?!http://)([a-zA-Z]+)", ",http://$1");
        tmp  =  tmp.replaceFirst("^http://", "");
        String[] lnkargs  =  tmp.split(",http://");
        for (String url : lnkargs) {
                url  =  url.startsWith("wwww.")? url.substring(4):url;
                host_click.add(parse_host(url));
                link_click.add(url);
        }
        int k = 0, m = 0;
        while(k  <  link_show.size() && m <  link_click.size()){
            if (link_show.get(k).equals(link_click.get(m))){
                pos_clicks.add(true);
                k++;
                m++;
            }else{
                pos_clicks.add(false);
                k++;
            }
        }
        for (int i = k; i < link_show.size(); ++i)
            pos_clicks.add(false);
        timestamps  =  new ArrayList <>();
        final String[] timestamps_args  =  args[idx++].split(',');
        for (final String timestamp : timestamps_args) {
            timestamps.add(Long.parseLong(timestamp));
        }
        if (timestamps.size()  =  =  link_click.size()) {
            this.timings  =  true;
            for (int i  =  0; i  <  link_click.size() - 1; ++i) {
                final Long time  =  (timestamps.get(i + 1) - timestamps.get(i)) / 1000;
                time_map.put(link_click.get(i), time.intValue());
            }
            time_map.put(link_click.get(link_click.size() - 1), 30*60);
        }
        query  =  parse_query(query);
    }

    @Override
    public void write(final DataOutput out){
        out.writeUTF(query);
        out.writeInt(geolocation);

        out.writeInt(link_show.size());
        for (int i  =  0; i  <  link_show.size(); i++) {
            out.writeUTF(link_show.get(i));
        }

        out.writeInt(link_click.size());
        for (int i  =  0; i  <  link_click.size(); i++) {
            out.writeUTF(link_click.get(i));
        }

        out.writeInt(timestamps.size());
        for (int i  =  0; i  <  timestamps.size(); i++) {
            out.writeLong(timestamps.get(i));
        }
    }

    @Override
    public void readFields(final DataInput in) {
        query  =  in.readUTF();
        geolocation  =  in.readInt();

        link_click  =  new ArrayList < >();
        final int link_clickSize  =  in.readInt();
        for (int i  =  0; i  <  link_clickSize; i++) {
            link_click.add(in.readUTF());
        }

        timestamps  =  new ArrayList < >();
        final int timestampsSize  =  in.readInt();
        for (int i  = 0; i <  timestampsSize; i++)
        {
            timestamps.add(in.readLong());
        }
        
        link_show  =  new ArrayList < >();
        final int link_showSize  =  in.readInt();
        for (int i  =  0; i  <  link_showSize; i++) {
            link_show.add(in.readUTF());
        }

    }
}

public class Click_model extends Configured implements Tool {

    public static final String docs  =  "docs";
    public static final String query-docs  =  "query-docs";
    public static final String hosts  =  "hosts";
    public static final String query-hosts  =  "query-hosts";
    public static final String queries  =  "queries";
    
    static private Map <String, String> get_url(final Mapper.Context context, final Path pt) {
        final Map <String, String> map  =  new HashMap < >();
        try {
            final FileSystem fs  =  FileSystem.get(context.getConfiguration());
            final BufferedReader buffer  =  new BufferedReader(new InputStreamReader(fs.open(pt),"UTF8"));
            String line;
            while ((line  =  buffer.readLine()) ! =  null) {
                final String[] arr =  line.split("\t");
                String s  =  arr[1].charAt(arr[1].length() - 1)  =  =  '/' ? arr[1].substring(0, arr[1].length() - 1) : arr[1];
                s  =  s.startsWith("www.") ? s.substring(4) : s;
                map.put(s, arr[0]);
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }

        return map;
    }

    static private Map <String, String> get_host(final Mapper.Context context, final Path pt) {
        final Map <String, String> map  =  new HashMap < >();
        try {
            final FileSystem fs  =  FileSystem.get(context.getConfiguration());
            final BufferedReader buffer  =  new BufferedReader(new InputStreamReader(fs.open(pt),"UTF8"));
            String line;
            while ((line  =  buffer.readLine()) ! =  null) {
                final String[] arr =  line.split("\t");
                int ind  =  arr[1].toString().indexOf("/");
                String h  =   arr[1].substring(0,ind);
                h  =  h.startsWith("www.") ? h.substring(4) : h;
                map.put(h, arr[0]);
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }

        return map;
    }

    static private Map <String, String> get_queries(final Mapper.Context context, final Path pt) {
        final Map <String, String> map  =  new HashMap < >();
        try {
            final FileSystem fs  =  FileSystem.get(context.getConfiguration());
            final BufferedReader buffer  =  new BufferedReader(new InputStreamReader(fs.open(pt), "UTF8"));
            String line;
            while ((line  =  buffer.readLine()) ! =  null) {
                final String[] arr =  line.split("\t");
                map.put(arr[1].trim(), arr[0]);
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }

        return map;
    }

    public static class Mapper extends Mapper <LongWritable, Text, Text, Text> {

        static Map <String, String> ids;
        static Map <String, String> hosts;
        static Map <String, String> queries;
        static Map <String, String> queries_yandex;

        @Override
        protected void setup(final Context context) {
            super.setup(context);
            final Path urls  =  new Path("ipynb/url.data");
            final Path q  =  new Path("ipynb/queries.tsv");
            final Path qspell  =  new Path("ipynb/Yandex_spell.txt");
            ids  =  get_url(context, urls);
            queries  =  get_queries(context, q);
            queries_yandex  =   get_queries(context, qspell);
            hosts  =  get_host(context, urls);

        }

        @Override
        protected void map(final LongWritable key, final Text value, final Context context) {
            try {
                final Click_Pattern pattern  =  new Click_Pattern();
                pattern.parse_str(value.toString());
                boolean flag_query  =  true;
                String qid  =  queries.get(pattern.query);
                if (qid =  = null){
                    qid =  queries_yandex.get(pattern.query);
                    if (qid =  = null)
                        flag_query = false;
                }
                if (flag_query){
                    int[] res  =  new int[6];
                    res[0]  =  pattern.link_show.size();
                    res[1]  =  pattern.link_click.size();
                    if (pattern.timings){
                        int time  =  0;
                        for(int i = 0; i <  pattern.link_click.size();++i){
                            time += pattern.time_map.get(pattern.link_click.get(i)); 
                        }
                        res[2] = time;
                    }
                    int mean  =  0;
                    for(int i = 0; i < pattern.link_click.size();++i)
                        mean += pattern.link_show.indexOf(pattern.link_click.get(i))+1;
                    if (pattern.link_click.size()! = 0){
                        res[3] = pattern.link_show.indexOf(pattern.link_click.get(0))+1;
                        res[4]  =  mean / pattern.link_click.size();
                    }else{
                        res[5] = 1;
                    }
                    String str  =  String.valueOf(res[0])+','+String.valueOf(res[1])+','+String.valueOf(res[2])+','+String.valueOf(res[3])+','+
                    String.valueOf(res[4])+','+String.valueOf(res[5]);
                    context.write(new Text("queries,"+qid), new Text(str));
                }
                for (int i  =  0; i  <  pattern.link_show.size(); i++) {
                        if (ids.containsKey(pattern.link_show.get(i))) {
                            int[] res  =  new int[11];
                            if (pattern.pos_clicks.get(i)){
                                res[0] = 1;
                                if (pattern.link_click.indexOf(pattern.link_show.get(i)) == 0)
                                    res[1] = 1;
                                if (pattern.link_click.indexOf(pattern.link_show.get(i)) == pattern.link_click.size()-1)
                                    res[2] = 1;
                                res[4] = pattern.link_click.indexOf(pattern.link_show.get(i))+1;
                                if (pattern.timings)
                                    res[5] = pattern.time_map.get(pattern.link_show.get(i));
                                int k = 0, m = i-1;
                                while(m>0 && !pattern.pos_clicks.get(m)){--m;++k;}
                                res[9] =  m> = 0 ? k: 0;
                                k = 0; 
                                m = i+1;
                                while(m < pattern.pos_clicks.size() && !pattern.pos_clicks.get(m)){++m;++k;}
                                res[10] = m! = pattern.pos_clicks.size() ? k : 0;
                            }
                            if (pattern.link_click.size() == 0)
                                res[6] = 1;
                            for(int j =  0; j  <  pattern.link_click.size();++j){
                                if (pattern.link_show.indexOf(pattern.link_click.get(j)) < i)
                                    break;
                                if (pattern.link_show.indexOf(pattern.link_click.get(j))>i){
                                    res[6] = 1;
                                    break;
                                }
                                if (i! = 0)
                                    res[7] = pattern.pos_clicks.get(i-1) ? 1 : 0;
                                if (i! = pattern.link_show.size()-1)
                                    res[8]  = pattern.pos_clicks.get(i+1) ? 1 : 0;
                            }
                            res[3] = i+1;
                            String str = String.valueOf(res[0])+","+String.valueOf(res[1])+","+String.valueOf(res[2])+","+
                                String.valueOf(res[3])+","+String.valueOf(res[4])+","+String.valueOf(res[5])+","+String.valueOf(res[6])+
                                ","+String.valueOf(res[7])+","+String.valueOf(res[8])+","+String.valueOf(res[9])+","+String.valueOf(res[10]);                            
                            if (flag_query) {
                                context.write(new Text("query-docs,"+qid+'\t'+String.valueOf(ids.get(pattern.link_show.get(i)))),new Text(str));
                            }
                            context.write(new Text("docs,"+String.valueOf(ids.get(pattern.link_show.get(i)))),new Text(str));
                        }
                    }
                    for (int i  =  0; i  <  pattern.host_show.size(); i++) {
                        if (hosts.containsKey(pattern.host_show.get(i))) {
                            int[] res  =  new int[11];
                            if (pattern.pos_clicks.get(i)){
                                res[0] = 1;
                                if (pattern.host_click.indexOf(pattern.host_show.get(i)) == 0)
                                    res[1] = 1;
                                if (pattern.host_click.indexOf(pattern.host_show.get(i)) == pattern.host_click.size()-1)
                                    res[2] = 1;
                                res[4] = pattern.host_click.indexOf(pattern.host_show.get(i))+1;
                                if (pattern.timings){
                                    res[5] = pattern.time_map.get(pattern.link_show.get(i));
                                int k = 0,m = i-1;
                                while(m>0 && !pattern.pos_clicks.get(m)){--m;++k;}
                                res[9] = k;
                                k = 0; m = i+1;
                                while(m < pattern.pos_clicks.size() && !pattern.pos_clicks.get(m)){++m;++k;}
                                res[10] =  k;
                                }
                            }
                            if (pattern.host_click.size() =  = 0)
                                res[6] = 1;
                            for(int j =  0; j  <  pattern.link_click.size();++j){
                                if (pattern.host_show.indexOf(pattern.host_click.get(j)) < i)
                                    break;
                                if (pattern.host_show.indexOf(pattern.host_click.get(j))>i){
                                    res[6] = 1;
                                    break;
                                }
                                if (i>0)
                                    res[7] = pattern.pos_clicks.get(i-1) ? 1 : 0;
                                if (i < pattern.link_show.size()-1)
                                    res[8]  = pattern.pos_clicks.get(i+1) ? 1 : 0;
                            }
                            res[3] = i+1;
                            String str = String.valueOf(res[0])+","+String.valueOf(res[1])+","+String.valueOf(res[2])+","+
                                String.valueOf(res[3])+","+String.valueOf(res[4])+","+String.valueOf(res[5])+","+String.valueOf(res[6])+
                                ","+String.valueOf(res[7])+","+String.valueOf(res[8])+","+String.valueOf(res[9])+","+String.valueOf(res[10]);
                            if (flag_query){
                                context.write(new Text("query-hosts,"+qid+'\t'+String.valueOf(hosts.get(pattern.host_show.get(i)))),new Text(str));
                            } 
                            context.write(new Text("hosts,"+String.valueOf(hosts.get(pattern.host_show.get(i)))),new Text(str));
                        }
                    }
            } catch (final Exception e) {
                e.printStackTrace();
                return;
            }
        }

    }

    public static class Reducer extends Reducer <Text, Text, Text, Text> {

        private MultipleOutputs <Text, Text> multipleOutputs;

        public void setup(final Reducer.Context context) {
            multipleOutputs  =  new MultipleOutputs(context);

        }

        @Override
        protected void reduce(final Text key, final Iterable < Text> nums, final Context context)
                throws IOException, InterruptedException {
            String[] keys  =  key.toString().split("\\,");
            if (keys[0].equals("queries")){
                int count_q =  0;
                int shows_q =  0;
                int clicks_q =  0;
                int time  =  0;
                int first_clicks_q  =  0;
                int shows_noclick_q  =  0;
                for(Text str : nums){
                    String[] data  =  str.toString().split("\\,");
                    count_q+ = 1;
                    shows_q+ = Integer.valueOf(data[0]);
                    clicks_q+ = Integer.valueOf(data[1]);
                    time+ = Integer.valueOf(data[2]);
                    first_clicks_q+ = Integer.valueOf(data[3]);
                    shows_noclick_q+ = Integer.valueOf(data[5]);
                }
                String res  =  "count_query:"+String.valueOf(count_q)+ "\tshows_docs:" + String.valueOf(shows_q)+"\tclicks_docs:"+String.valueOf(clicks_q)+
                "\ttime:"+String.valueOf(time)+"\tfirst_clicks:"+String.valueOf(first_clicks_q)+"\tavg_pos_click:"+String.valueOf(avg_pos_click)+
                "\tshows_noclick:"+String.valueOf(shows_noclick_q);
                multipleOutputs.write(new Text(keys[1]), new Text(res.trim()), queries+"/part");
            } else {
            int[] pos_clicks =  new int[7];
            int[] pos_shows =  new int[7];
            int clicks  =  0;
            int shows =  0;
            double time  =  0.;
            int first_clicks  =  0;
            int last_clicks =  0;
            int beforeclick = 0;
            int afterclick  =  0;
            int shows_not_top = 0;
            int clicks_not_top = 0;
            for (final Text str : nums) {
                String[] data  =  str.toString().split("\\,");
                int click  =  Integer.parseInt(data[0]);
                int pos_show  =  Integer.parseInt(data[3]);
                int pos_click  =  Integer.parseInt(data[4]);
                int fclick  =  Integer.parseInt(data[1]);
                int lckick  =  Integer.parseInt(data[2]);
                time+ = Integer.parseInt(data[5]);
                beforeclick+ = Integer.parseInt(data[9]);;
                afterclick  =  Integer.parseInt(data[10]);;
                shows++;
                clicks += click;
                first_clicks += fclick;
                last_clicks += lckick;
                if (pos_show < 6){
                    pos_shows[pos_show]+= 1;
                }
                else{    
                    pos_shows[6]+= 1;
                }
                if (pos_click < 6)
                    pos_clicks[pos_click]+= 1;
                else
                    pos_clicks[6] += 1;
            }
            shows_not_top = shows;
            clicks_not_top = clicks;
            for(int i = 1;i < 7;++i){
                shows_not_top -= pos_shows[i];
                clicks_not_top -= pos_clicks[i];
            }
            String res  =  "shows:"+String.valueOf(shows)+"\tclicks:"+String.valueOf(clicks)+"\ttime:"+String.valueOf(time)+"\tfirst_clicks:"+String.valueOf(first_clicks)+
                "\tlast_clicks:"+String.valueOf(last_clicks)+"\tbefore_clicks:"+String.valueOf(beforeclick)+"\tafter_clicks:"+String.valueOf(afterclick)+"\tshows_not_top"+String.valueOf(shows_not_top)+"\tclicks_not_top"+String.valueOf(clicks_not_top)+"\tpos_clicks:";
            for(int i = 1;i < 7;++i){
                res += String.valueOf(pos_clicks[i])+" ";
            }
            res+= "\tpos_shows:";
            for(int i = 1;i < 7;++i){
                res += String.valueOf(pos_shows[i])+" ";
            }
            switch(keys[0]){
            case("docs"):
                multipleOutputs.write(new Text(keys[1]), new Text(res.trim()), docs+"/part");
                break;
            case("query-docs"):
                multipleOutputs.write(new Text(keys[1]), new Text(res.trim()), query-docs+"/part");
                break;
            case("hosts"):
                multipleOutputs.write(new Text(keys[1]), new Text(res.trim()), hosts+"/part");
                break;
            case("query-hosts"):
                multipleOutputs.write(new Text(keys[1]), new Text(res.trim()), query-hosts+"/part");
                break;
            default:
                break;
            }
        }
        }
    }

    private Worker worker_config(final String input, final String output) throws IOException {
        final Worker worker  =  Worker.getInstance(getConf());
        worker.setJarByClass(Click_model.class);
        worker.setInputFormatClass(TextInputFormat.class);
        worker.setInputFormatClass(TextInputFormat.class);
        worker.setMapOutputValueClass(Text.class);
        worker.setMapOutputKeyClass(Text.class);
        worker.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(worker, new Path(input));
        FileOutputFormat.setOutputPath(worker, new Path(output));
        MultipleOutputs.addNamedOutput(worker, docs, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(worker, query-docs, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(worker, hosts, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(worker, query-hosts, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(worker, queries, TextOutputFormat.class, Text.class, Text.class);
        worker.setMapperClass(Mapper.class);
        worker.setReducerClass(Reducer.class);
        worker.setNumReduceTasks(1);
        return worker;
    }

    @Override
    public int run(final String[] args) throws Exception {
        
        final FileSystem fs  =  FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        final Worker worker  =  worker_config(args[0], args[1]);
        return worker.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(final String[] args) throws Exception {
        final int ret  =  ToolRunner.run(new Click_model(), args);
        System.exit(ret);
    }
}