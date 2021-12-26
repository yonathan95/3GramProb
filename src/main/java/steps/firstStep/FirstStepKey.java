package steps.firstStep;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;


public class FirstStepKey implements WritableComparable<FirstStepKey> {
    private Text firstWord;
    private Text secondWord;
    private Text thirdWord;

    public FirstStepKey() {
        this.firstWord = new Text();
        this.secondWord = new Text();
        this.thirdWord = new Text();
    }

    public FirstStepKey(Text otherFirstWord, Text otherSecondWord, Text otherThirdWord) {
        this.firstWord = new Text(otherFirstWord.toString());
        this.secondWord = new Text(otherSecondWord.toString());
        this.thirdWord = new Text(otherThirdWord.toString());
    }

    public FirstStepKey(String otherFirstWord, String otherSecondWord, String otherThirdWord) {
        this.firstWord = new Text(otherFirstWord);
        this.secondWord = new Text(otherSecondWord);
        this.thirdWord = new Text(otherThirdWord);
    }

    public void setFields(String otherFirstWord, String otherSecondWord, String otherThirdWord) {
        this.firstWord.set(otherFirstWord);
        this.secondWord.set(otherSecondWord);
        this.thirdWord.set(otherThirdWord);
    }

    public FirstStepKey(FirstStepKey otherFirstKey) {
        this.firstWord = otherFirstKey.getFirstWord();
        this.secondWord = otherFirstKey.getSecondWord();
        this.thirdWord = otherFirstKey.getThirdWord();
    }

    public Text getFirstWord() {
        return this.firstWord;
    }

    public Text getSecondWord() {
        return this.secondWord;
    }

    public Text getThirdWord() {
        return this.thirdWord;
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        ((Writable) firstWord).readFields(in);
        ((Writable) secondWord).readFields(in);
        ((Writable) thirdWord).readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        ((Writable) firstWord).write(out);
        ((Writable) secondWord).write(out);
        ((Writable) thirdWord).write(out);
    }

    @Override
    public int compareTo(FirstStepKey otherFirstStepKey) { //C0 -> C1 -> C2 -> (2,1,0)
        String tFirstW = firstWord.toString();
        String tSecondW = firstWord.toString();
        String tThirdW = firstWord.toString();
        String oFirstW = otherFirstStepKey.getFirstWord().toString();
        String oSecondW = otherFirstStepKey.getSecondWord().toString();
        String oThirdW = otherFirstStepKey.getThirdWord().toString();

        if (tFirstW.equals("*") && tSecondW.equals("*") && tThirdW.equals("*")) {
            return -1;

        } else if (oFirstW.equals("*") && oSecondW.equals("*") && oThirdW.equals("*")) {
            return 1;

        } else if (tSecondW.equals("*") && tThirdW.equals("*")) {
            if (oSecondW.equals("*") && oThirdW.equals("*")) {
                return tFirstW.compareTo(oFirstW);
            } else {
                return -1;
            }

        } else if (tFirstW.equals("*") && tThirdW.equals("*")) {
            if (oFirstW.equals("*") && oThirdW.equals("*")) {
                return tSecondW.compareTo(oSecondW);
            } else {
                return -1;
            }

        } else if (tFirstW.equals("*") && tSecondW.equals("*")) {
            if (oFirstW.equals("*") && oSecondW.equals("*")) {
                return tThirdW.compareTo(oThirdW);
            } else {
                return -1;
            }
        } else if (tFirstW.equals("*")) {
            if (oFirstW.equals("*")) {
                if (tSecondW.compareTo(oSecondW) == 0) {
                    return tThirdW.compareTo(oThirdW);
                }
                return tSecondW.compareTo(oSecondW);
            } else {
                return -1;
            }


        } else if (tThirdW.equals("*")) {
            if (oThirdW.equals("*")) {
                if (tSecondW.compareTo(oSecondW) == 0) {
                    return tFirstW.compareTo(oFirstW);
                }
                return tSecondW.compareTo(oSecondW);
            } else {
                return -1;
            }


        } else {
            int firstC = tFirstW.compareTo(oFirstW);
            int secondC = tSecondW.compareTo(oSecondW);
            int thirdC = tThirdW.compareTo(oThirdW);
            if (firstC != 0) {
                return firstC;
            }
            if (secondC != 0) {
                return secondC;
            }
            return thirdC;
        }
    }

    public String toString() {
        return this.firstWord.toString() + " " + this.secondWord.toString() + " " + this.thirdWord.toString();
    }

    public int getCode() {
        return firstWord.hashCode() + secondWord.hashCode() + thirdWord.hashCode();
    }
}
