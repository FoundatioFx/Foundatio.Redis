﻿using System;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Logging;
using Foundatio.Logging.InMemory;
using Foundatio.Logging.NLog;
using Foundatio.Queues;
using Foundatio.Utility;
using StackExchange.Redis;

namespace Foundatio.SampleJobClient {
    public class Program {
        private static readonly object _writeLock = new object();
        private static readonly InMemoryLoggerFactory _loggerFactory = new InMemoryLoggerFactory();
        private static readonly ILogger _log = _loggerFactory.CreateLogger("Program");
        private static IQueue<PingRequest> _queue;

        public static void Main(string[] args) {
            _loggerFactory.AddNLog();
            Console.CursorVisible = false;
            StartDisplayingLogMessages();

            _queue = new RedisQueue<PingRequest>(new RedisQueueOptions<PingRequest> { ConnectionMultiplexer = ConnectionMultiplexer.Connect("localhost") });

            var tokenSource = new CancellationTokenSource();
            CancellationToken token = tokenSource.Token;

            WriteOptionsMenu();

            while (true) {
                Console.SetCursorPosition(0, OPTIONS_MENU_LINE_COUNT + 1);
                ConsoleKeyInfo keyInfo = Console.ReadKey(true);

                if (keyInfo.Key == ConsoleKey.D1) {
                    EnqueuePing();
                } else if (keyInfo.Key == ConsoleKey.D2) {
                    EnqueuePing(100);
                } else if (keyInfo.Key == ConsoleKey.D3) {
                    if (tokenSource.IsCancellationRequested) {
                        tokenSource = new CancellationTokenSource();
                        token = tokenSource.Token;
                    }

                    Task.Run(() => EnqueueContinuousPings(25, token), token);
                } else if (keyInfo.Key == ConsoleKey.Q) {
                    break;
                } else if (keyInfo.Key == ConsoleKey.S) {
                    tokenSource.Cancel();
                    ClearOutputLines();
                }
            }
        }

        private static void EnqueuePing(int count = 1) {
            for (int i = 0; i < count; i++)
                _queue.EnqueueAsync(new PingRequest { Data = "b", PercentChanceOfException = 0 }).GetAwaiter().GetResult();

            _log.Info(() => $"Enqueued {count} ping requests");
        }

        private static void EnqueueContinuousPings(int count, CancellationToken token) {
            do {
                for (int i = 0; i < count; i++)
                    _queue.EnqueueAsync(new PingRequest { Data = "b", PercentChanceOfException = 0 }).GetAwaiter().GetResult();

                _log.Info(() => $"Enqueued {count} ping requests");
            } while (!token.IsCancellationRequested);
        }

        private const int OPTIONS_MENU_LINE_COUNT = 6;
        private static void WriteOptionsMenu() {
            lock (_writeLock) {
                Console.SetCursorPosition(0, 0);
                ClearConsoleLines(0, OPTIONS_MENU_LINE_COUNT - 1);
                Console.WriteLine("1: Enqueue 1");
                Console.WriteLine("2: Enqueue 100");
                Console.WriteLine("3: Enqueue continuous");
                Console.WriteLine();
                Console.WriteLine("S: Stop");
                Console.WriteLine("Q: Quit");
            }
        }

        private static void ClearOutputLines(int delay = 1000) {
            Task.Run(() => {
                SystemClock.Sleep(delay);
                ClearConsoleLines(OPTIONS_MENU_LINE_COUNT, OPTIONS_MENU_LINE_COUNT + 4);
            });
        }

        private const int LOG_LINE_COUNT = 10;
        private static void StartDisplayingLogMessages() {
            Task.Factory.StartNew(() => {
                while (true) {
                    var logEntries = _loggerFactory.GetLogEntries(LOG_LINE_COUNT);
                    lock (_writeLock) {
                        ClearConsoleLines(OPTIONS_MENU_LINE_COUNT + 5, OPTIONS_MENU_LINE_COUNT + 6 + LOG_LINE_COUNT);
                        Console.SetCursorPosition(0, OPTIONS_MENU_LINE_COUNT + 6);
                        foreach (var logEntry in logEntries) {
                            var originalColor = Console.ForegroundColor;
                            Console.ForegroundColor = GetColor(logEntry);
                            Console.WriteLine(logEntry);
                            Console.ForegroundColor = originalColor;
                        }
                    }
                    SystemClock.Sleep(250);
                }
            });
        }

        private static ConsoleColor GetColor(LogEntry logEntry) {
            switch (logEntry.LogLevel) {
                case LogLevel.Debug:
                    return ConsoleColor.Gray;
                case LogLevel.Error:
                    return ConsoleColor.Yellow;
                case LogLevel.Information:
                    return ConsoleColor.White;
                case LogLevel.Trace:
                    return ConsoleColor.DarkGray;
                case LogLevel.Warning:
                    return ConsoleColor.Magenta;
                case LogLevel.Critical:
                    return ConsoleColor.Red;
            }

            return ConsoleColor.White;
        }

        private static void ClearConsoleLines(int startLine = 0, int endLine = -1) {
            if (endLine < 0)
                endLine = Console.WindowHeight - 2;

            lock (_writeLock) {
                int currentLine = Console.CursorTop;
                int currentPosition = Console.CursorLeft;

                for (int i = startLine; i <= endLine; i++) {
                    Console.SetCursorPosition(0, i);
                    Console.Write(new string(' ', Console.WindowWidth));
                }
                Console.SetCursorPosition(currentPosition, currentLine);
            }
        }
    }

    public class PingRequest {
        public string Data { get; set; }
        public string Id { get; set; } = Guid.NewGuid().ToString("N");
        public int PercentChanceOfException { get; set; } = 0;
    }
}
