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
                    ,SourceRowValidationStatus.DateNotContiguous
                        => "date was not contiguous with previous row"
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

            EventStoreClient? client = null;

            if (eventdb)
            {
                client = new EventStoreClient(
                    EventStoreClientSettings.Create(
                        "esdb://localhost:2113?tls=false"
                    )
                );
            }

            var builder = new Builder(
                 (eventdb ? new ResultsToEvents(client!) : new ResultsToFile())
                ,new AllScenarios(
                    new BaseDays(
                        eventdb ? new SourceFromEvents(client!) : new SourceFromFile()
                    )
                )
            );

            Console.WriteLine(
                "CObs build: reading daily source data..."
            );

            IReadActionResult status = await builder.ReadDaysRawAsync();

            if (
                (!status.Success)
            ||  (builder.BaseDays.DaysRaw.Count
                    <= builder.Scenarios.MedianTimeToMortalityValues.Max())
            ) {
                if (!status.Success)
                {
                    if (status.Status.RowNumber == 0)
                    {
                        Console.WriteLine(
                            "CObs build: access exception reading Source Data: "
                          + status.Message
                        );
                    }
                    else
                    {
                        ReportValidationError(status.Status);
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
                      + (builder.Scenarios.MedianTimeToMortalityValues.Max() + 1).ToString()
                      + "."
                    );
                }            

                if (keyToExit)
                {
                    Console.WriteLine("\nPress any key to exit.");
                    Console.ReadKey();
                }

                Environment.Exit(1);
            }

            Console.WriteLine(
                "CObs build: populating rolling averages..."
            );

            builder.PopulateRolling();

            Console.WriteLine(
                "CObs build: generating scenarios..."
            );

            builder.GenerateScenarios();

            Console.WriteLine(
                "CObs build: running scenarios..."
            );

            builder.RunScenarios();

            Console.WriteLine(
                "CObs build: extracting result days..."
            );

            builder.ExtractResultDays();

            Console.WriteLine(
                "CObs build: extracting global aggregates..."
            );

            builder.ExtractAggregates();

            Console.WriteLine(
                "CObs build: committing results..."
            );

            await builder.CommitResultsAsync();

            Console.WriteLine(
                "CObs build: committing aggregates..."
            );

            await builder.CommitAggregatesAsync();

            Console.WriteLine("CObs build: Done.");

            if (keyToExit)
            {
                Console.WriteLine("\nPress any key to exit.");
                Console.ReadKey();
            }

            Environment.Exit(0);
        }
    }
}
