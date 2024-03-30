package practise;

import java.util.*;

public class bucketSort {
    public static void main(String[] args) {
        int[] arr={1,2,1,2,1,2,2,100,1};
        HashMap<Integer, Integer> hashMap=new HashMap<>();
        HashMap<Integer, ArrayList<Integer>> maxMap=new HashMap<>();


/*        for (int i = 0; i < arr.length; i++) {
            //System.out.println(arr[i]);
        }*/

        int counter=1;

        for(Integer i : arr){
            //System.out.println(i);
            int count=hashMap.getOrDefault(i,0)+1;
            hashMap.put(i,count);
            maxMap.put(counter++, new ArrayList<>());
        }

        for(HashMap.Entry<Integer, Integer> i: hashMap.entrySet()){
            int element=i.getKey();
            int count=i.getValue();
            //ArrayList<Integer> intermediate=maxMap.getOrDefault(count,new ArrayList<>());
            //intermediate.add(element);
            maxMap.get(count).add(element);

        }

        int no=2;
        for (int i = maxMap.size(); i > 0 ; i--) {

            if(!maxMap.get(i).isEmpty() && no!=0){
                System.out.println("Maximum elements"+ maxMap.get(i)+" with count "+i);
                no--;
            }
        }

    }
}
