using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Linq;
using System.Threading.Tasks;
using System.IO;
using EventStore.Client;

namespace CObs
{
    public interface IReadActionResult
    {
        bool                   Success      { get; }
        string                 Message      { get; }
        SourceValidationStatus Status       { get; }
        List<DayRaw>           DaysRaw      { get; }
        ulong                  ReadPosition { get; }
    }

    public interface IReadEventsAsync
    {
        Task<IReadActionResult> ReadDaysRawAsync();
    }

    public interface ICommitActionResult
    {
        bool   Success { get; }
        string Message { get; }
    }

    public interface ICommitEventsAsync
    {
        Task<bool>                IsBuildDequeued();
        Task<ICommitActionResult> RegisterBuild();
        Task<ICommitActionResult> CommitResultsAsync(
             List<ResultsDay> pResultsDays
            ,Aggregates       pAggregates
            ,ulong            pReadPosition
        );
    }

    class ReadActionResult : IReadActionResult
    {
        public bool                   Success      { get; private set; }
        public string                 Message      { get; private set; }
        public SourceValidationStatus Status       { get; private set; }
        public List<DayRaw>           DaysRaw      { get; private set; }
        public ulong                  ReadPosition { get; private set; }

        public ReadActionResult(
             bool                   pSuccess
            ,string                 pMessage
            ,SourceValidationStatus pStatus
            ,List<DayRaw>           pDaysRaw
            ,ulong                  pReadPosition
        ) {
            Success      = pSuccess;
            Message      = pSuccess ? ""       : pMessage;
            Status       = pSuccess
                ? new SourceValidationStatus(true, 0, SourceRowValidationStatus.OK)
                : pStatus;
            DaysRaw      = pSuccess ? pDaysRaw : new List<DayRaw>();
            ReadPosition = pSuccess ? pReadPosition : 0;
        }
    }

    class CommitActionResult : ICommitActionResult
    {
        public bool   Success { get; private set; }
        public string Message { get; private set; }

        public CommitActionResult(
             bool         pSuccess
            ,string       pMessage
        ) {
            Success = pSuccess;
            Message = pSuccess ? "" : pMessage;
        }
    }

    public class SourceFromFile : IReadEventsAsync
    {
        public async Task<IReadActionResult> ReadDaysRawAsync()
        {
            int       index    = 0;
            DateTime? lastDate = null;
            var       daysRaw  = new List<DayRaw>();

            try
            {
                /* read raw source data */
                using var sr = new StreamReader("SourceData.txt");
                
                string? line;

                while ((line = await sr.ReadLineAsync()) != null)
                {
                    List<string> row = line.Split(',').ToList().Select(
                        entry => entry.Trim()
                    ).ToList();

                    SourceRowValidationStatus status = BaseDays.ValidateSourceRow(
                         row
                        ,lastDate
                    );

                    if (status != SourceRowValidationStatus.OK)
                    {
                        /* report validation error */
                        return new ReadActionResult(
                             false
                            ,"row validation error"
                            ,new SourceValidationStatus(
                                 false
                                ,(index + 1) /* line number hints start from 1 */
                                ,status
                            )
                            ,new List<DayRaw>()
                            ,0
                        );
                    }

                    var day = new DayRaw(index, row);

                    daysRaw.Add(day);

                    lastDate = day.Date;

                    index++;
                }                
            }
            catch (Exception e)
            {
                /* report access exception */
                return new ReadActionResult(
                     false
                    ,e.Message
                    ,new SourceValidationStatus(
                         false
                        ,0
                        ,SourceRowValidationStatus.ExceptionReadingEvents
                    )
                    ,new List<DayRaw>()
                    ,0
                );
            }

            /* return souce events */
            return new ReadActionResult(
                 true
                ,""
                ,new SourceValidationStatus(true, 0, SourceRowValidationStatus.OK)
                ,daysRaw
                ,0
            );
        }
    }

    public class ResultsToFile : ICommitEventsAsync
    {
        public Task<bool> IsBuildDequeued()
        {
            return Task.FromResult(false);
        }

        public Task<ICommitActionResult> RegisterBuild()
        {
            return Task.FromResult(
                new CommitActionResult(true, "") as ICommitActionResult
            );
        }

        private static async Task<ICommitActionResult> CommitResultsDaysAsync(
            List<ResultsDay> pResultsDays
        ) {
            try
            {
                using (var w = new StreamWriter("ResultsData.txt"))
                {
                    foreach (ResultsDay day in pResultsDays)
                    {
                        string line
                          = day.TimelineIndex
                          + " ,"
                          + day.Date.ToString("yyyy-MM-dd")
                          + " ,"
                          + day.Mortality
                          + " ,"
                          + day.Hospitalizations
                          + " ,"
                          + day.Tests
                          + " ,"
                          + day.Positivity
                          + " ,"
                          + day.Rolling5DayMortality
                          + " ,"
                          + day.Rolling5DayHospitalizations
                          + " ,"
                          + day.Rolling5DayTests
                          + " ,"
                          + day.Rolling5DayPositivity
                          + " ,"
                          + (int)day.LowerBoundSourcedOn
                          + " ,"
                          + (int)day.BaselineSourcedOn
                          + " ,"
                          + (int)day.UpperBoundSourcedOn
                          + " ,"
                          + day.AdmissionsWithChurnLowerBound
                          + " ,"
                          + day.AdmissionsWithChurnBaseline
                          + " ,"
                          + day.AdmissionsWithChurnUpperBound
                          + " ,"
                          + day.ActualDNCLowerBound
                          + " ,"
                          + day.ActualDNCBaseline
                          + " ,"
                          + day.ActualDNCUpperBound
                          + " ,"
                          + day.Rolling9DayDeltaCDeltaTLowerBound
                          + " ,"
                          + day.Rolling9DayDeltaCDeltaTBaseline
                          + " ,"
                          + day.Rolling9DayDeltaCDeltaTUpperBound
                          + " ,"
                          + day.GrowthRateLowerBound
                          + " ,"
                          + day.GrowthRateBaseline
                          + " ,"
                          + day.GrowthRateUpperBound
                          + " ,"
                          + day.REffLowerBound
                          + " ,"
                          + day.REffBaseline
                          + " ,"
                          + day.REffUpperBound
                          + " ,"
                          + day.DoublingTimeLowerBound
                          + " ,"
                          + day.DoublingTimeBaseline
                          + " ,"
                          + day.DoublingTimeUpperBound;

                        await w.WriteLineAsync(line);
                        await w.FlushAsync();
                    }

                    w.Close();
                }

                File.Copy("ResultsData.txt", "CObsResults/ResultsData.txt", true);
            }
            catch (Exception e)
            {
                return new CommitActionResult(
                     false
                    ,"exception writing results to store: " + e.Message
                );
            }

            return new CommitActionResult(true, "");
        }

        private static async Task<ICommitActionResult> CommitAggregatesAsync(
            Aggregates pAggregates
        ) {
            try
            {
                using (var w = new StreamWriter("Aggregates.txt"))
                {
                    string line
                      = pAggregates.CurrentREffLowerBound
                      + " ,"
                      + pAggregates.CurrentREffBaseline
                      + " ,"
                      + pAggregates.CurrentREffUpperBound
                      + " ,"
                      + pAggregates.CurrentDoublingTimeLowerBound
                      + " ,"
                      + pAggregates.CurrentDoublingTimeBaseline
                      + " ,"
                      + pAggregates.CurrentDoublingTimeUpperBound
                      + " ,"
                      + ((pAggregates.CurrentDoublingTimeUnstable) ? 1 : 0)
                      + " ,"
                      + pAggregates.ProjectedTotalSeroprevLowerBound
                      + " ,"
                      + pAggregates.ProjectedTotalSeroprevBaseline
                      + " ,"
                      + pAggregates.ProjectedTotalSeroprevUpperBound
                      + " ,"
                      + pAggregates.ProjectedTotalMortalityLowerBound
                      + " ,"
                      + pAggregates.ProjectedTotalMortalityBaseline
                      + " ,"
                      + pAggregates.ProjectedTotalMortalityUpperBound;

                    await w.WriteLineAsync(line);
                    await w.FlushAsync();

                    w.Close();
                }

                File.Copy("Aggregates.txt", "CObsResults/Aggregates.txt", true);
            }
            catch (Exception e)
            {
                return new CommitActionResult(
                     false
                    ,"exception writing aggregates to store: " + e.Message
                );
            }

            return new CommitActionResult(true, "");
        }

        public async Task<ICommitActionResult> CommitResultsAsync(
             List<ResultsDay> pResultsDays
            ,Aggregates       pAggregates
            ,ulong            pReadPosition
        ) {
            var result = await CommitResultsDaysAsync(pResultsDays);

            if (!result.Success) { return result; }

            return await CommitAggregatesAsync(pAggregates);
        }
    }

    public class SourceFromEvents : IReadEventsAsync
    {
        public EventStoreClient Client     { get; private set; }
        public string           StreamName { get; private set; }

        public SourceFromEvents(
             EventStoreClient pClient
            ,string           pStreamName
        ) { Client = pClient; StreamName = pStreamName; }

        public async Task<IReadActionResult> ReadDaysRawAsync()
        {
            var daysRaw                   = new List<DayRaw>();
            int                  index    = 0;
            DateTime?            lastDate = null;
            List<ResolvedEvent>? events;

            try
            {
                var result = Client.ReadStreamAsync(
                     Direction.Forwards
                    ,StreamName + "-source"
                    ,StreamPosition.Start
                );

                if (await result.ReadState != ReadState.Ok)
                {
                    throw new Exception("source stream empty");
                }

                events = await result.ToListAsync();
            }
            catch (Exception e)
            {
                /* report access exception */
                return new ReadActionResult(
                     false
                    ,e.Message
                    ,new SourceValidationStatus(
                         false
                        ,0
                        ,SourceRowValidationStatus.ExceptionReadingEvents
                    )
                    ,new List<DayRaw>()
                    ,0
                );
            }

            foreach (var sourceDay in events)
            {
                SourceDayRoot? day;

                try
                {
                    day = JsonSerializer.Deserialize<SourceDayRoot>(
                        Encoding.UTF8.GetString(sourceDay.Event.Data.ToArray())!
                    )!;
                }
                catch (Exception e)
                {
                    /* report type exception */
                    return new ReadActionResult(
                         false
                        ,e.Message
                        ,new SourceValidationStatus(
                             false
                            ,(index + 1) /* line number hints start from 1 */
                            ,SourceRowValidationStatus.ExceptionReadingEvents
                        )
                        ,new List<DayRaw>()
                        ,0
                    );
                }

                SourceRowValidationStatus status = BaseDays.ValidateSourceEvent(
                     day.sourceDay!
                    ,lastDate
                );

                if (status != SourceRowValidationStatus.OK)
                {
                    /* report validation error */
                    return new ReadActionResult(
                         false
                        ,"row validation error"
                        ,new SourceValidationStatus(
                             false
                            ,(index + 1) /* line number hints start from 1 */
                            ,status
                        )
                        ,new List<DayRaw>()
                        ,0
                    );
                }

                var dayRaw = new DayRaw(index, day.sourceDay!);

                daysRaw.Add(dayRaw);

                lastDate = dayRaw.Date;

                index++;
            }

            /* return souce events */
            return new ReadActionResult(
                 true
                ,""
                ,new SourceValidationStatus(true, 0, SourceRowValidationStatus.OK)
                ,daysRaw
                ,events.Last().OriginalEventNumber.ToUInt64()
            );
        }
    }

    public class ResultsToEvents : ICommitEventsAsync
    {
        class BuildEvent
        {
            public DateTime Timestamp { get; private set; }

            public BuildEvent() { Timestamp = DateTime.Now; }
        }

        class ResultsReady
        {
            public ulong ReadPosition { get; private set; }

            public ResultsReady(ulong pReadPosition) { ReadPosition = pReadPosition; }
        }

        public EventStoreClient Client        { get; private set; }
        public string           StreamName    { get; private set; }
        public StreamRevision   BuildPosition { get; private set; }

        public ResultsToEvents(EventStoreClient pClient, string pStreamName) {
            Client        = pClient;
            StreamName    = pStreamName;
            BuildPosition = new StreamRevision();
        }

        public async Task<bool> IsBuildDequeued()
        {
            var result = Client.ReadStreamAsync(
                 Direction.Backwards
                ,StreamName + "-results"
                ,StreamPosition.End
                ,1
            );

            if (await result.ReadState == ReadState.StreamNotFound) { return true; }

            var head = await result.FirstOrDefaultAsync();

            if (head.OriginalEventNumber.ToUInt64() != BuildPosition.ToUInt64())
            {
                return true;
            }

            return false;
        }

        public async Task<ICommitActionResult> RegisterBuild()
        {
            try
            {
                var result = await Client.AppendToStreamAsync(
                     StreamName + "-results"
                    ,StreamState.Any
                    ,new List<EventData> { new EventData(
                         Uuid.NewUuid()
                        ,"build-event"
                        ,JsonSerializer.SerializeToUtf8Bytes(
                            new BuildEvent()
                        )
                    ) }
                );

                BuildPosition = result.NextExpectedStreamRevision;
            }
            catch (Exception e)
            {
                return new CommitActionResult(
                     false
                    ,"exception registering build: " + e.Message
                );
            }

            return new CommitActionResult(true, "");
        }

        public async Task<ICommitActionResult> CommitResultsAsync(
             List<ResultsDay> pResultsDays
            ,Aggregates       pAggregates
            ,ulong            pReadPosition
        ) {
            var results = pResultsDays.Select(day => {
                return new EventData(
                     Uuid.NewUuid()
                    ,"results-day-received"
                    ,JsonSerializer.SerializeToUtf8Bytes(day)
                );
            }).ToList();

            results.Add(
                new EventData(
                     Uuid.NewUuid()
                    ,"aggregates-received"
                    ,JsonSerializer.SerializeToUtf8Bytes(pAggregates)
                )
            );

            results.Add(
                new EventData(
                     Uuid.NewUuid()
                    ,"results-ready"
                    ,JsonSerializer.SerializeToUtf8Bytes(
                        new ResultsReady(pReadPosition)
                    )
                )
            );

            try
            {
                await Client.AppendToStreamAsync(
                     StreamName + "-results"
                    ,BuildPosition
                    ,results
                );
            }
            catch (Exception e)
            {
                return new CommitActionResult(
                     false
                    ,"exception writing results to store: " + e.Message
                );
            }

            return new CommitActionResult(true, "");
        }
    }
}
