package it.polimi.ds.client;

import it.polimi.ds.rmi.RemoteInfo;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Client {
    private Client() {
    }

    public static void main(String[] args) {
        try {
            String initialHost = (args.length < 1) ? null : args[0];
            if (initialHost == null) throw new IllegalArgumentException("You need to specify a host!");

            String initialRegistryName = (args.length < 2) ? null : args[1];
            if (initialRegistryName == null) throw new IllegalArgumentException("You need to specify a registry name!");

            RemoteInfo initialRemoteInfo = new RemoteInfo(initialHost, initialRegistryName);

            Middleware middleware = new LeaderlessMiddleware(new DefaultConnector(), initialRemoteInfo);

            System.out.println("Use commands \"get(key)\", \"put(key, value)\", or \"exit\".");

            Pattern putPattern = Pattern.compile("^put\\(([^,]+),\\s*([^)]+)\\)$");
            Pattern getPattern = Pattern.compile("^get\\(([^)]+)\\)$");
            Matcher putMatcher;
            Matcher getMatcher;

            Scanner scanner = new Scanner(System.in);
            String input, key, value;
            do {

                System.out.print("Insert command: ");
                input = scanner.nextLine();
                putMatcher = putPattern.matcher(input);
                getMatcher = getPattern.matcher(input);

                if (putMatcher.matches()) {
                    key = putMatcher.group(1);
                    value = putMatcher.group(2);
                    System.out.println("PUT - Key: " + key + " - Value: " + value);
                    if (middleware.Put(key, value)) {
                        System.out.println("Response: success");
                    } else {
                        System.out.println("Response: fail");
                    }

                } else if (getMatcher.matches()) {
                    key = getMatcher.group(1);
                    System.out.println("GET - Key: " + key);
                    System.out.println("Response: " + middleware.Get(key));

                } else if (!input.equalsIgnoreCase("exit")) {
                    System.out.println("Invalid input.");
                }

            } while (!input.equalsIgnoreCase("exit"));

            scanner.close();
            middleware.close();
            System.out.println("Bye.");
            System.exit(0);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}