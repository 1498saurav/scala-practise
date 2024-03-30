package practise;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;


public class RebelReach {
    public static void main(String[] args) throws Exception {
        Scanner s = new Scanner(System.in);
        int no_of_testcases = Integer.parseInt(s.nextLine());
        System.out.println("no_of_testcases: " + no_of_testcases);

        int no_of_cities = Integer.parseInt(s.nextLine());
        System.out.println("no_of_cities: " + no_of_cities);

        HashMap<Integer, ArrayList<Integer>> tree = new HashMap<>();

        for (int i = 1; i < no_of_cities + 1; i++) {
            tree.put(i, new ArrayList<>());
        }

        for (int i = 0; i < no_of_cities - 1; i++) {
            String[] arr = s.nextLine().split("\\s");
            System.out.println(arr[0] + " " + arr[1]);

            int leaf = Integer.parseInt(arr[1]);
            int node = Integer.parseInt(arr[0]);

            ArrayList<Integer> staging = tree.get(leaf);
            staging.add(node);

            tree.put(leaf, staging);
        }

        System.out.println(tree);

        String[] no_of_gaurds = s.nextLine().split("\\s");

        System.out.println(Arrays.toString(no_of_gaurds));

        int no_of_queries = Integer.parseInt(s.nextLine());
        System.out.println("no_of_queries: " + no_of_queries);

        for (int i = 0; i < no_of_queries; i++) {
            String[] query = s.nextLine().split("\\s");
            int city = Integer.parseInt(query[0]);
            int protestors = Integer.parseInt(query[1]);
            System.out.println(city + ":" + protestors);
            int current_in_city;
            boolean rebel=true;
            while (rebel) {
                int no_of_gaurds_present = Integer.parseInt(no_of_gaurds[city - 1]);
                protestors = no_of_gaurds_present - protestors;
                current_in_city = city;
                if (protestors < 0) {
                    protestors = Math.abs(protestors);
                    city = tree.get(city).get(0);
                }else{
                    System.out.println(current_in_city);
                    rebel=false;
                }
            }


        }

    }
}
