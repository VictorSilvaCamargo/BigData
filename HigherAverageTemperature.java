package advanced.customwritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;
public class HigherAverageTemperature {
    public static void main(String args[]) throws IOException,
            ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
// arquivos de entrada
        Path input = new Path(files[0]); // inputfireforest.csv
        Path intermediate = new Path(files[1]); // arquivo com a media por mês
// arquivo de saida
        Path output = new Path(files[2]); // arquivo com a maior média
// criacao do job1
        Job j1 = new Job(c, "media");
// registro das classes
        j1.setJarByClass(HigherAverageTemperature.class);
        j1.setMapperClass(MapForAverage.class);
        j1.setReducerClass(ReduceForAverage.class);
        j1.setCombinerClass(CombineForAverage.class);
// definição dos tipos de saída (map e reduce)
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(FireAvgTempWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(FloatWritable.class);
// cadastrar os arquivos de entrada e saída
        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);
// lanca o job1 e aguarda sua execucao
        j1.waitForCompletion(true);
// criacao do job2
        Job j2 = new Job(c, "maior media");
// registro das classes
        j2.setJarByClass(HigherAverageTemperature.class);
        j2.setMapperClass(MapForHigherAverage.class);
        j2.setReducerClass(ReduceForHigherAverage.class);
// definição dos tipos de saída (map e reduce)
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(FireHigherWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(FloatWritable.class);
// cadastrar os arquivos de entrada e saída
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);
// lanca o job1 e aguarda sua execucao
        j2.waitForCompletion(true);
    }
    public static class MapForAverage extends Mapper<LongWritable, Text, Text,
            FireAvgTempWritable> {
        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
// obtendo o conteúdo da linha e convertendo para string
            String linha = value.toString();
// quebrando a linha em colunas
            String[] colunas = linha.split(",");
// pegando somente a temperatura (index 8)
            float temperatura = Float.parseFloat(colunas[8]);
// pegando o mês (atividade 2)
            String mes = colunas[2];
// ocorrência
            int n = 1;
// passando a temperatura por mês (atividade 2)
            con.write(new Text(mes), new FireAvgTempWritable(temperatura, 1));
        }
    }
    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable,
            Text, FloatWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context
                con)
                throws IOException, InterruptedException {
// Chega no reduce, uma chave ÚNICA com uma lista de valores do tipo
// FireAvgTempWritable (temperatura, ocorrencia)
            float somaTemp=0;
            int somaN=0;
            for(FireAvgTempWritable v:values){
                somaTemp+=v.getSomaTemperatura();
                somaN+=v.getOcorrencia();
            }
// calcula a media
            float media = somaTemp/somaN;
// escreve o resultado final no HDFS
            con.write(key, new FloatWritable(media));
        }
    }
    // Implementação do combiner (mini-reducer)
    public static class CombineForAverage extends Reducer<Text,
            FireAvgTempWritable, Text, FireAvgTempWritable>{
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context
                con) throws IOException, InterruptedException {
// no combiner, vamos somar as temperaturas parciais do bloco e também
            as ocorrências
            float somaTemp=0;
            int somaN=0;
            for(FireAvgTempWritable v:values){
                somaTemp+=v.getSomaTemperatura();
                somaN+=v.getOcorrencia();
            }
// passando para o reduce, os resultados parciais obtidos em cada
            mapper
            con.write(key, new FireAvgTempWritable(somaTemp, somaN));
        }
    }
    public static class MapForHigherAverage extends Mapper<LongWritable, Text,
            Text, FireHigherWritable>{
        public void map(LongWritable key, Text value, Context con) throws
                IOException, InterruptedException {
// pega o conteudo da linha do arquivo gerado pela primeira rotina
            MapReduce
            String linha = value.toString();
// separa o conteúdo da linha em colunas
            String[] colunas = linha.split("\t"); // LEMBRAR
            String mes = colunas[0];
            float temperatura = Float.parseFloat(colunas[1]);
            con.write(new Text("Maior"), new FireHigherWritable(mes, temperatura));
        }
    }
    public static class ReduceForHigherAverage extends Reducer<Text,
            FireHigherWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FireHigherWritable> values, Context
                con) throws IOException, InterruptedException {
// busca pela maior temperatura
            float maiorTemp = Float.MIN_VALUE;
            String maiorMes = null;
            for(FireHigherWritable v:values){
                float temp = v.getTemperatura();
                String mes = v.getMes();
                if(temp > maiorTemp){
                    maiorMes = mes;
                    maiorTemp = temp;
                }
            }
            con.write(new Text(maiorMes), new FloatWritable(maiorTemp));
        }
    }
}