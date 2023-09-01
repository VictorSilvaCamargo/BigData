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
// passando temperatura e ocorrência para o sort/shuffle com uma chave
        única "media"
        con.write(new Text("media"), new FireAvgTempWritable(temperatura, 1));
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
}