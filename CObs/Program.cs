using System;
using System.Threading.Tasks;
using System.Linq;
using EventStore.Client;

/*
*
* Author:  lmw.gh.2020@gmail.com, all rights reserved, October 2020
* License: Apache License, Version 2.0
*
* https://opensource.org/licenses/Apache-2.0 
*
*/

namespace CObs
{
    class Program
    {
        static void ReportValidationError(SourceValidationStatus pStatus)
        {
            if (!pStatus.SourceOK)
            {
                Console.WriteLine(
                    "CObs build: validation error reading Source Data at row "
                  + pStatus.RowNumber
                  + "."
                );

                string error = pStatus.RowStatus switch {
                     SourceRowValidationStatus.OK
                        => "no errors found"
                    ,SourceRowValidationStatus.ExceptionReadingEvents
                        => "couldn't read source data events"
                    ,SourceRowValidationStatus.WrongNumberOfColumns
                        => "had wrong number of columns"
                    ,SourceRowValidationStatus.DateUnreadable
                        => "had unreadable date"
                    ,SourceRowValidationStatus.DNCUnreadable
                        => "had unreadable DNC"
                    ,SourceRowValidationStatus.DNCNegative
                        => "had negative DNC"
                    ,SourceRowValidationStatus.TestsUnreadable
                        => "had unreadable Tests"
                    ,SourceRowValidationStatus.TestsNegative
                        => "had negative Tests"
                    ,SourceRowValidationStatus.PositivityUnreadable
                        => "had unreadable Positivity"
                    ,SourceRowValidationStatus.PositivityNotBetweenZeroAndOneHundred
                        => "had Positivity not between 0 and 100"
                    ,SourceRowValidationStatus.MortalityUnreadable
                        => "had unreadable Mortality"
                    ,SourceRowValidationStatus.MortalityNegative
                        => "had negative Mortality"
                    ,SourceRowValidationStatus.HospitalizationsUnreadable
                        => "had unreadable Hospitalizations"
                    ,SourceRowValidationStatus.HospitalizationsNegative
                        => "had negative Hospitalizations"
                    ,_
                        => "had unknown error"
                };

                Console.WriteLine("CObs build: row " + error + ".");
            }
        }

        static async Task Main(string[] args)
        {
            /*
                Process command line arguments and run build.
            */
            bool keyToExit = !args.ToList().Any(arg => arg == "nokey");
            bool eventdb   = args.ToList().Any( arg => arg == "eventdb");
            bool series    = args.ToList().Any( arg => arg == "series");

            if (!eventdb) { series = false; }

            const string streamName = "outbreak";

            EventStoreClient? client = null;

            if (eventdb)
            {
                string connectionString = Environment.GetEnvironmentVariable(
                    "DB_CONNECTION_STRING"
                ) ?? "esdb://localhost:2113?tls=false";

                client = new EventStoreClient(
                    EventStoreClientSettings.Create(connectionString)
                );
            }
            
            var builder = new Builder(
                 (eventdb ? new ResultsToEvents(client!, streamName) : new ResultsToFile())
                ,new BaseDays(
                    eventdb
                        ? new SourceFromEvents(client!, streamName)
                        : new SourceFromFile()
                )
                ,series
            );

            Console.WriteLine(
                "CObs build: registering build..."
            );

            ICommitActionResult registerStatus = await builder.RegisterBuild();

            if (!registerStatus.Success)
            {
                Console.WriteLine(
                    "CObs build: access exception registering Build: "
                  + registerStatus.Message
                );

                if (client != null) { client.Dispose(); }

                if (keyToExit)
                {
                    Console.WriteLine("\nPress any key to exit.");
                    Console.ReadKey();
                }

                Environment.Exit(1);
            }

            Console.WriteLine(
                "CObs build: reading daily source data..."
            );

            var scenarios  = new AllScenarios();
            var readStatus = await builder.ReadDaysRawAsync();

            if (
                (!readStatus.Success)
            ||  (builder.BaseDays.DaysRaw.Count
                    <= scenarios.MedianTimeToMortalityValues.Max())
            ) {
                if (!readStatus.Success)
                {
                    if (readStatus.Status.RowNumber == 0)
                    {
                        Console.WriteLine(
                            "CObs build: error reading Source Data: "
                          + readStatus.Message
                        );
                    }
                    else
                    {
                        ReportValidationError(readStatus.Status);
                    }
                }
                else
                {
                    Console.WriteLine(
                        "CObs build: "
                      + "daily data must contain more rows than max median time to mortality."
                    );

                    Console.WriteLine(
                        "CObs build: minimum number of rows is therefore currently: "
                      + (scenarios.MedianTimeToMortalityValues.Max() + 1).ToString()
                      + "."
                    );
                }

                if (client != null) { client.Dispose(); }

                if (keyToExit)
                {
                    Console.WriteLine("\nPress any key to exit.");
                    Console.ReadKey();
                }

                Environment.Exit(1);
            }

            int jobNumber = 1;

            foreach (var job in builder.BuildQueue)
            {
                var nOfm = "Cobs build ("
                    + jobNumber
                    + "/"
                    + builder.BuildQueue.Count
                    + "): ";

                if (await builder.CommitAdapter.IsBuildDequeued()) { Environment.Exit(1); }

                Console.WriteLine(
                    nOfm + "populating rolling averages..."
                );

                job.PopulateRolling();

                if (await builder.CommitAdapter.IsBuildDequeued()) { Environment.Exit(1); }

                Console.WriteLine(
                    nOfm + "generating scenarios..."
                );

                job.GenerateScenarios();

                if (await builder.CommitAdapter.IsBuildDequeued()) { Environment.Exit(1); }

                Console.WriteLine(
                    nOfm + "running scenarios..."
                );

                await job.RunScenarios();

                if (await builder.CommitAdapter.IsBuildDequeued()) { Environment.Exit(1); }

                Console.WriteLine(
                    nOfm + "extracting result days..."
                );

                job.ExtractResultDays();

                if (await builder.CommitAdapter.IsBuildDequeued()) { Environment.Exit(1); }

                Console.WriteLine(
                    nOfm + "extracting global aggregates..."
                );

                job.ExtractAggregates();

                jobNumber++;
            }

            if (await builder.CommitAdapter.IsBuildDequeued()) { Environment.Exit(1); }

            Console.WriteLine(
                "CObs build: committing results..."
            );

            var commitStatus = await builder.CommitResultsAsync();

            if (!commitStatus.Success)
            {
                Console.WriteLine(
                    "CObs build: exception committing Results Data: "
                  + commitStatus.Message
                );
            }
            else
            {
                Console.WriteLine("CObs build: Done.");
            }

            if (client != null) { client.Dispose(); }

            if (keyToExit)
            {
                Console.WriteLine("\nPress any key to exit.");
                Console.ReadKey();
            }

            Environment.Exit(0);
        }
    }
}
