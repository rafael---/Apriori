/*
 * Trabalho de Tópicos especiais em bancos de dados
 * Acadêmico: Rafael Hengen Ribeiro
 * Descrição: Implementação do algoritmo Apriori utilizando as bibliotecas de MapReduce do Hadoop
 * 
 * Classe: FilterMapper
 * Função: Mapeamento de itens e contagem de linhas
 */

package initialFilter;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import apriori.Apriori;

public class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String [] tokens = line.split("[ \t]");
            
            Apriori.lineNumber++;

            for (String s : tokens) 
                context.write(new Text(s), new Text("1"));
          
        }
    }