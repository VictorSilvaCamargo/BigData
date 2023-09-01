package advanced.customwritable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
/*
Precisamos que essa nova classe seja serializável (Writable) para a transmissão
dos dados entre os DataNodes
No Hadoop, o tipo Writable é sempre um Java Bean.
Java Bean é caracterizado por:
- ter um construtor padrão (vazio),
- atributos privados,
- getters e setters para cada atributo
LEMBRETE: a classe deve ser comparável com ela mesma (etapa sort/shuffle)
*/
public class FireHigherWritable implements WritableComparable<FireHigherWritable> {
    private String mes;
    private float temperatura;
    public FireHigherWritable() {
    }
    public FireHigherWritable(String mes, float temperatura) {
        this.mes = mes;
        this.temperatura = temperatura;
    }
    public String getMes() {
        return mes;
    }
    public void setMes(String mes) {
        this.mes = mes;
    }
    public float getTemperatura() {
        return temperatura;
    }
    public void setTemperatura(float temperatura) {
        this.temperatura = temperatura;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FireHigherWritable that = (FireHigherWritable) o;
        return Float.compare(that.temperatura, temperatura) == 0 &&
                Objects.equals(mes, that.mes);
    }
    @Override
    public int hashCode() {
        return Objects.hash(mes, temperatura);
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(mes);
        dataOutput.writeFloat(temperatura);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        mes = dataInput.readUTF();
        temperatura = dataInput.readFloat();
    }
    @Override
    public String toString() {
        return "FireHigherWritable{" +
                "mes='" + mes + '\'' +
                ", temperatura=" + temperatura +
                '}';
    }
// método que realiza o comparativo entre diferentes objetos na etapa de
    sort/shuffle
    // para a ordenação de acordo com as chaves
// dado 2 objetos, comparar se um é maior que o outro, menor ou igual com base
    no hashCode (valores dos atributos)
    @Override
    public int compareTo(FireHigherWritable o) {
        if(this.hashCode() < o.hashCode()) {
            return -1;
        }else if(this.hashCode() > o.hashCode()){
            return 1;
        }
        return 0;
    }
}