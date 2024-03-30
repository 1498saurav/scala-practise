package practise;

import java.util.HashMap;

public class characterReplacement {
    public static void main(String[] args) {

        String str="ABBBAAAB";
        int replacement=1;

        int maxWindow=0;
        //initialise a alphabet set
        HashMap<Character, Integer> hashMap=new HashMap<>();

        int left=0;

        for (int i = 0; i < str.length(); i++) {
            char currentChar=str.charAt(i);
            int countOfCurrentCharacter=hashMap.getOrDefault(currentChar,0) + 1;
            hashMap.put(currentChar,countOfCurrentCharacter);

            System.out.println(currentChar+" : "+countOfCurrentCharacter);
            maxWindow=Math.max(maxWindow,countOfCurrentCharacter);
            if(left-i+1-maxWindow>replacement){
                char leftChar=str.charAt(left);
                hashMap.put(leftChar,hashMap.get(leftChar) - 1);
                left++;
            }

        }

        System.out.println(maxWindow);

    }
}
