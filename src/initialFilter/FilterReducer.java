/*
 * Trabalho de Tópicos especiais em bancos de dados
 * Acadêmico: Rafael Hengen Ribeiro
 * Descrição: Implementação do algoritmo Apriori utilizando as bibliotecas de MapReduce do Hadoop
 * 
 * Classe: FilterReducer
 * Função: Cálculo do suporte e seleção de itens com suporte mínimo
 */
package initialFilter;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import apriori.Apriori;

 public class FilterReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.valueOf(value.toString());
            }

            double suporte = (double)sum/Apriori.lineNumber;
            
            if(suporte >= Apriori.minSup)	{
            	Apriori.conjItens.put(Integer.valueOf(key.toString()), suporte);
            	context.write(key, new Text(String.valueOf(suporte)));
            }
        }
    }