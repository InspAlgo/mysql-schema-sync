package com.github.inspalgo;

import com.github.inspalgo.command.Arguments;
import org.fusesource.jansi.AnsiConsole;
import picocli.CommandLine;

/**
 * @author InspAlgo
 * @date 2021/1/7 20:19 UTC+08:00
 */
public class Main {

    public static void main(String[] args) {
        AnsiConsole.systemInstall();
        new CommandLine(new Arguments()).execute(args);
        AnsiConsole.systemUninstall();
    }
}
