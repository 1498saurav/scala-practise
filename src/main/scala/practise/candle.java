package practise;

public class candle {
    public static void main(String[] args) {
        int candleFromBurnt=4;
        int candles=7;
        int remainingWax=candles;

        do {
            int newCandles = remainingWax / candleFromBurnt;
            if (newCandles > 0) {
                candles = candles + newCandles;
                remainingWax = remainingWax - (candleFromBurnt * newCandles) + newCandles;
            }
            System.out.println("No of remaining wax: " + remainingWax);
        } while (remainingWax >= candleFromBurnt);

        System.out.println("No of candles: "+candles);
    }
  
    
}
