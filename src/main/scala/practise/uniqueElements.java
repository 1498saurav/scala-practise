package practise;

import java.util.HashMap;

public class uniqueElements {
    public static void main(String[] args) {
        int[] arr={1,2,2,1,1,3};

        HashMap<Integer,Integer> hashMap=new HashMap<>();


        for(Integer i: arr){
            int count=hashMap.getOrDefault(i,0);
            hashMap.put(i,count+1);
        }

        System.out.println(hashMap.values());

    }
}
